use std::collections::BTreeMap;
use std::time::Duration;

use redis::{Pipeline, PipelineCommands, Commands};
use r2d2::{Pool, Config};
use r2d2_redis::RedisConnectionManager;

use rand::Rng;

use futures_cpupool;

use chan::{sync, after, tick, WaitGroup, Receiver, Sender};
use chan_signal::{Signal as SysSignal, notify};

use libc::getpid;

use chrono::UTC;

use serde_json::to_string;

use futures::sync::mpsc::channel;
use futures::{Future, BoxFuture};
use futures::future::{ok, err};

use random_choice::random_choice;
use serde_json::from_str;

use errors::*;
use errors::ErrorKind::*;
use utils::rust_gethostname;
use middleware::MiddleWare;
use job_handler::JobHandler;
use RedisPool;
use job::Job;
use job_agent::JobAgent;
use FutureJob;

#[derive(Default)]
pub struct SidekiqServerBuilder<'a> {
    concurrency: usize,
    middlewares: Vec<Box<MiddleWare + 'a>>,
    job_handlers: BTreeMap<String, Box<JobHandler + 'a>>,
    queues: Vec<String>,
    weights: Vec<f64>,
}

impl<'a> SidekiqServerBuilder<'a> {
    pub fn new() -> SidekiqServerBuilder<'a> {
        SidekiqServerBuilder { concurrency: 10, ..Default::default() }
    }
    pub fn concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.concurrency = concurrency;
        self
    }
    pub fn middleware<M>(&mut self, middleware: M) -> &mut Self
        where M: MiddleWare + 'a
    {
        self.middlewares.push(Box::new(middleware));
        self
    }
    pub fn job_handler<H>(&mut self, name: &str, handler: H) -> &mut Self
        where H: JobHandler + 'a
    {
        self.job_handlers.insert(name.to_string(), Box::new(handler));
        self
    }
    pub fn queue(&mut self, name: &str, weight: f64) -> &mut Self {
        self.queues.push(name.to_string());
        self.weights.push(weight);
        self
    }
    pub fn build(&self, redis: &str) -> Result<SidekiqServer> {
        SidekiqServer::with_builder(self, redis)
    }
}

pub struct SidekiqServer<'a> {
    redis_pool: RedisPool,
    worker_pool: futures_cpupool::CpuPool,
    pub namespace: String,
    job_handlers: BTreeMap<String, Box<JobHandler + 'a>>,
    middlewares: Vec<Box<MiddleWare + 'a>>,
    queues: Vec<String>,
    weights: Vec<f64>,
    started_at: f64,
    rs: String,
    pid: usize,
    signal_chan: Receiver<SysSignal>,
    worker_info: BTreeMap<String, bool>, // busy?
    concurrency: usize,
    termination_wg: WaitGroup,
    pub force_quite_timeout: usize,
}

impl<'a> SidekiqServer<'a> {
    // Interfaces to be exposed
    pub fn with_builder(builder: &SidekiqServerBuilder, redis: &str) -> Result<Self> {
        if builder.concurrency == 0 {
            bail!(ZeroConcurrency)
        }
        if builder.job_handlers.len() == 0 {
            bail!(NoJobHandler)
        }
        if builder.queues.len() == 0 {
            bail!(ZeroQueue)
        }

        let signal = notify(&[SysSignal::INT, SysSignal::USR1]); // should be here to set proper signal mask to all threads
        let now = UTC::now();
        let config = Config::builder()
            .pool_size(builder.concurrency as u32) // dunno why, it corrupt for unable to get connection sometimes with concurrency + 1
            .build();
        let redis_pool = Pool::new(config, RedisConnectionManager::new(redis)?)?;
        let worker_pool = futures_cpupool::Builder::new()
            .after_start(|| info!("Worker started")) //  info!("worker '{}' start working", self.with_server_id(&self.id));)
            .name_prefix("sidekiq-rs")
            .pool_size(builder.concurrency)
            .create();

        Ok(SidekiqServer {
            redis_pool: redis_pool,
            worker_pool: worker_pool,
            namespace: String::new(),
            job_handlers: BTreeMap::new(),
            queues: builder.queues.clone(),
            weights: builder.weights.clone(),
            started_at: now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1000000f64,
            pid: unsafe { getpid() } as usize,
            worker_info: BTreeMap::new(),
            concurrency: builder.concurrency,
            termination_wg: WaitGroup::new(),
            signal_chan: signal,
            force_quite_timeout: 10,
            middlewares: vec![],
            // random itentity
            rs: ::rand::thread_rng().gen_ascii_chars().take(12).collect(),
        })
    }

    pub fn start(&mut self) {
        info!("sidekiq-rs is running...");
        let signal = self.signal_chan.clone();

        // controller loop
        let clock = tick(Duration::from_secs(2)); // report to sidekiq every 2 secs

        loop {
            chan_select! {
                default => {
                    // TODO make jobs
                    match self.poll() {
                        Ok(Some(job)) => {
                            let fut = self.make_job(job);
                            let handle = self.worker_pool.spawn(fut);
                            handle.forget();
                        }
                        Ok(None) => {}
                        Err(e) => error! ("Poll job error {}", e),
                    }
                },
                signal.recv() -> signal => {
                    match signal {
                        Some(signal @ SysSignal::USR1) => {
                            info!("{:?}: Terminating", signal);
                            //self.terminate_gracefully();
                            break;
                        }
                        Some(signal @ SysSignal::INT) => {
                            info!("{:?}: Force terminating", signal);                            
                            //self.terminate_forcely();
                            break;
                        }
                        Some(_) => { unreachable!() }
                        None => { unreachable!() }
                    }
                },
                clock.recv() => {
                    debug!("server clock triggered");
                    if let Err(e) = self.report_alive() {
                        error!("report alive failed: '{}'", e);
                    }
                },
                // rsx.recv() -> sig => {
                //     debug!("received signal {:?}", sig);
                //     if let Some(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                //         error!("error when dealing signal: '{}'", e);
                //     }
                //     let worker_count = self.threadpool.active_count();
                //     // relaunch workers if they died unexpectly
                //     if worker_count < self.concurrency {
                //         warn!("worker down, restarting");
                //         self.launch_workers(tsx.clone(), rox.clone());
                //     } else if worker_count > self.concurrency {
                //         unreachable!("unreachable! worker_count can never larger than concurrency!")
                //     }
                // }
            }
        }

        // exiting
        info!("sidekiq exited");
    }

    // Worker start/terminate functions


    // fn launch_workers(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
    //     while self.worker_info.len() < self.concurrency {
    //         self.launch_worker(tsx.clone(), rox.clone());
    //     }
    // }


    // fn launch_worker(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
    //     let worker = SidekiqWorker::new(&self.identity(),
    //                                     self.redis_pool.clone(),
    //                                     tsx,
    //                                     rox,
    //                                     self.queues.clone(),
    //                                     self.weights.clone(),
    //                                     self.job_handlers
    //                                         .iter_mut()
    //                                         .map(|(k, v)| (k.clone(), v.cloned()))
    //                                         .collect(),
    //                                     self.middlewares.iter_mut().map(|v| v.cloned()).collect(),
    //                                     self.namespace.clone());
    //     self.worker_info.insert(worker.id.clone(), false);
    //     self.thread_pool.execute(move || worker.work());
    // }

    // fn inform_termination(&self, tox: Sender<Operation>) {
    //     for _ in 0..self.concurrency {
    //         tox.send(Operation::Terminate);
    //     }
    // }

    // fn terminate_forcely(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
    //     self.inform_termination(tox);

    //     let timer = after(Duration::from_secs(self.force_quite_timeout as u64));
    //     // deplete the signal channel
    //     loop {
    //         chan_select! {
    //             timer.recv() => {
    //                 info!("force quitting");
    //                 break
    //             },
    //             rsx.recv() -> sig => {
    //                 debug!("received signal {:?}", sig);
    //                 if let Some(Err(e)) = sig.map(|s| self.deal_signal(s)) {
    //                     error!("error when dealing signal: '{}'", e);
    //                 }
    //                 if self.worker_info.len() == 0 {
    //                     break
    //                 }
    //             },
    //         }
    //     }
    // }


    // fn terminate_gracefully(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
    //     self.inform_termination(tox);

    //     info!("waiting for other workers exit");
    //     // deplete the signal channel
    //     loop {
    //         chan_select! {
    //             rsx.recv() -> sig => {
    //                 debug!("received signal {:?}", sig);
    //                 if let Some(Err(e)) = sig.map(|s| self.deal_signal(s)) {
    //                     error!("error when dealing signal: '{}'", e);
    //                 }
    //                 if self.worker_info.len()== 0 {
    //                     break
    //                 }
    //             },
    //         }
    //     }
    // }


    // fn deal_signal(&mut self, sig: Signal) -> Result<()> {
    //     debug!("dealing signal {:?}", sig);
    //     match sig {
    //         Signal::Complete(id, n) => {
    //             let _ = try!(self.report_processed(n));
    //             *self.worker_info.get_mut(&id).unwrap() = false;
    //         }
    //         Signal::Fail(id, n) => {
    //             let _ = try!(self.report_failed(n));
    //             *self.worker_info.get_mut(&id).unwrap() = false;
    //         }
    //         Signal::Acquire(id) => {
    //             self.worker_info.insert(id, true);
    //         }
    //         Signal::Terminated(id) => {
    //             self.worker_info.remove(&id);
    //         }
    //     }
    //     debug!("signal dealt");
    //     Ok(())
    // }
}

impl<'a> SidekiqServer<'a> {
    fn make_job(&mut self, job: Job) -> BoxFuture<(), (JobAgent, Error)> {
        let agent = JobAgent::new(job);
        let mut continuation: FutureJob = ok(agent.clone()).boxed();

        for middleware in &mut self.middlewares {
            continuation = middleware.before(continuation).boxed();
        }

        continuation = if let Some(handler) = self.job_handlers.get_mut(&agent.class) {
            //self.report_working(&job)?;
            handler.cloned().perform(continuation).boxed()
            //    self.report_done()?;
        } else {
            warn!("unknown job class '{}'", agent.class);
            let errkind = UnknownJobClass(agent.class.clone());
            err((agent, errkind.into())).boxed()
        };

        for middleware in &mut self.middlewares {
            continuation = middleware.after(continuation).boxed();
        }
        continuation.and_then(|_| ok(())).boxed()
    }
}

impl<'a> SidekiqServer<'a> {
    fn poll(&mut self) -> Result<Option<Job>> {
        let mut choice = random_choice();

        let queue_name = {
            let v = choice.random_choice_f64(&self.queues, &self.weights, 1);
            v[0]
        };

        debug!("Polling queue {} once", queue_name);

        let modified_queue_name = self.queue_name(queue_name);

        let result: Option<Vec<String>> = self.redis_pool.get()?.brpop(&modified_queue_name, 2)?;

        if let Some(result) = result {
            let mut job: Job = from_str(&result[1])?;
            if let Some(ref mut retry_info) = job.retry_info {
                retry_info.retried_at = Some(UTC::now());
            }

            job.namespace = self.namespace.clone();

            Ok(Some(job))

        } else {
            Ok(None)
        }

    }
}

// reporter
impl<'a> SidekiqServer<'a> {
    // Sidekiq dashboard reporting functions
    fn report_alive(&mut self) -> Result<()> {
        let now = UTC::now();

        let content = vec![("info",
                            to_string(&json!({
                                "hostname": rust_gethostname().unwrap_or("unknown".into()),
                                "started_at": self.started_at,
                                "pid": self.pid,
                                "concurrency": self.concurrency,
                                "queues": self.queues.clone(),
                                "labels": [],
                                "identity": self.identity()
                            }))
                                .unwrap()),
                           ("busy", self.worker_info.values().filter(|v| **v).count().to_string()),
                           ("beat",
                            (now.timestamp() as f64 +
                             now.timestamp_subsec_micros() as f64 / 1000000f64)
                                .to_string())];
        let conn = self.redis_pool.get()?;
        try!(Pipeline::new()
            .hset_multiple(self.with_namespace(&self.identity()), &content)
            .expire(self.with_namespace(&self.identity()), 5)
            .sadd(self.with_namespace(&"processes"), self.identity())
            .query::<()>(&*conn));

        Ok(())

    }


    fn report_processed(&mut self, n: usize) -> Result<()> {
        let connection = self.redis_pool.get()?;
        let _: () = Pipeline::new().incr(self.with_namespace(&format!("stat:processed:{}",
                                               UTC::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:processed")), n)
            .query(&*connection)?;

        Ok(())
    }


    fn report_failed(&mut self, n: usize) -> Result<()> {
        let connection = self.redis_pool.get()?;
        let _: () = Pipeline::new()
            .incr(self.with_namespace(&format!("stat:failed:{}", UTC::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:failed")), n)
            .query(&*connection)?;
        Ok(())
    }


    fn identity(&self) -> String {
        let host = rust_gethostname().unwrap_or("unknown".into());
        let pid = self.pid;

        host + ":" + &pid.to_string() + ":" + &self.rs
    }


    fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }

    fn queue_name(&self, name: &str) -> String {
        self.with_namespace(&("queue:".to_string() + name))
    }
}
