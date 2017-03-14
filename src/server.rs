use std::collections::BTreeMap;
use std::time::Duration;

use redis::{Pipeline, PipelineCommands, Commands};
use r2d2::{Pool, Config};
use r2d2_redis::RedisConnectionManager;

use rand::Rng;

use futures_cpupool;

use chan::{tick, WaitGroup, Receiver};
use chan_signal::{Signal as SysSignal, notify};

use libc::getpid;

use chrono::UTC;

use serde_json::{to_string, Value as JValue};

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

thread_local! {
    pub static WORKER_ID: String = ::rand::thread_rng().gen_ascii_chars().take(9).collect(); 
}

#[derive(Default)]
pub struct SidekiqServerBuilder<'a> {
    concurrency: usize,
    middlewares: Vec<Box<MiddleWare + 'a>>,
    job_handlers: BTreeMap<String, Box<JobHandler + 'a>>,
    namespace: String,
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
    pub fn namespace(&mut self, namespace: &str) -> &mut Self {
        self.namespace = namespace.into();
        self
    }
    pub fn queue(&mut self, name: &str, weight: f64) -> &mut Self {
        self.queues.push(name.to_string());
        self.weights.push(weight);
        self
    }
    pub fn build(self, redis: &str) -> Result<SidekiqServer<'a>> {
        SidekiqServer::with_builder(self, redis)
    }
}

pub struct SidekiqServer<'a> {
    redis_pool: RedisPool,
    worker_pool: futures_cpupool::CpuPool,
    namespace: String,
    job_handlers: BTreeMap<String, Box<JobHandler + 'a>>,
    middlewares: Vec<Box<MiddleWare + 'a>>,
    queues: Vec<String>,
    weights: Vec<f64>,
    started_at: f64,
    rs: String,
    pid: usize,
    signal_chan: Receiver<SysSignal>,
    worker_info: BTreeMap<String, bool>,
    concurrency: usize,
    wg: WaitGroup,
}

impl<'a> SidekiqServer<'a> {
    // Interfaces to be exposed
    pub fn with_builder(SidekiqServerBuilder {
            concurrency, middlewares, job_handlers, namespace, queues, weights
        }: SidekiqServerBuilder<'a>, redis: &str) -> Result<Self> {
        if concurrency == 0 {
            bail!(ZeroConcurrency)
        }
        if job_handlers.len() == 0 {
            bail!(NoJobHandler)
        }
        if queues.len() == 0 {
            bail!(ZeroQueue)
        }

        let rs: String = ::rand::thread_rng().gen_ascii_chars().take(12).collect(); // random string
        let rs_cloned = rs.clone(); // for capture
        let rs_cloned2 = rs.clone(); // for capture
        let signal = notify(&[SysSignal::INT, SysSignal::USR1]); // should be here to set proper signal mask to all threads
        let now = UTC::now();
        let pid = unsafe { getpid() } as usize;
        let wg = WaitGroup::new();
        wg.add(concurrency as i32);
        let wg_worker = wg.clone();

        // redis pool config
        let config = Config::builder()
            .pool_size(concurrency as u32) // dunno why, it corrupt for unable to get connection sometimes with concurrency + 1
            .build();

        // redis pool
        let redis_pool = Pool::new(config, RedisConnectionManager::new(redis)?)?;

        // worker pool
        let worker_pool = futures_cpupool::Builder::new()
            .after_start(move || {
                WORKER_ID.with(|id| {
                    info!("Worker '{}' start working", server_id(pid, &rs_cloned, &id));
                })
            })
            .before_stop(move || {
                WORKER_ID.with(|id| {
                    wg_worker.done();
                    info!("Worker '{}' stopped", server_id(pid, &rs_cloned2, &id));
                })
            })
            .name_prefix("sidekiq-rs")
            .pool_size(concurrency)
            .create();

        Ok(SidekiqServer {
            redis_pool: redis_pool,
            worker_pool: worker_pool,
            namespace: namespace,
            job_handlers: job_handlers,
            queues: queues,
            weights: weights.clone(),
            started_at: now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1000000f64,
            pid: pid,
            worker_info: BTreeMap::new(),
            concurrency: concurrency,
            signal_chan: signal,
            wg: wg,
            middlewares: middlewares,
            // random itentity
            rs: rs,
        })
    }

    pub fn start(mut self) {
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
                            let fut = self.pack_job(job);
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
                            // Just exit, destructor will do the things for us
                            break;
                        }
                        Some(signal @ SysSignal::INT) => {
                            info!("{:?}: Force terminating", signal);   
                            // Just exit, destructor will do the things for us
                            break;
                        }
                        Some(_) => { unreachable!() }
                        None => { unreachable!() }
                    }
                },
                clock.recv() => {
                    trace!("server clock triggered");
                    if let Err(e) = self.report_alive() {
                        error!("report alive failed: '{}'", e);
                    }
                },
            }
        }
        let wg = self.wg.clone();
        drop(self);
        wg.wait();
        info!("sidekiq-rs exited");
    }
}

impl<'a> SidekiqServer<'a> {
    fn pack_job(&mut self, job: Job) -> BoxFuture<(), Error> {
        let agent = JobAgent::new(job);
        let mut continuation: FutureJob = ok(agent.clone()).boxed();

        for middleware in &mut self.middlewares {
            continuation = middleware.before(continuation).boxed();
        }


        let worker_key_ = self.with_namespace(&self.with_server_id("workers")); // will be cloned twice after because two future uses it, and put it outside of the if to make borrowck happier
        continuation = if let Some(handler) = self.job_handlers.get_mut(&agent.class) {

            let pool = self.redis_pool.clone();
            let worker_key = worker_key_.clone();
            // report a worker is doing a job
            continuation = continuation.map(move |job| {
                    let conn = pool.get().unwrap();
                    let payload: JValue = json!({
                        "queue": job.queue.clone(),
                        "payload": *job,
                        "run_at": UTC::now().timestamp()
                    });
                    let _: Result<()> = Pipeline::new()
                        .hset(&worker_key,
                              &WORKER_ID.with(|id| id.clone()),
                              to_string(&payload).unwrap())
                        .expire(&worker_key, 5)
                        .query(&*conn)
                        .map_err(|err| err.into());
                    job
                })
                .boxed();


            continuation = handler.cloned().perform(continuation).boxed(); // Here the job is performed


            let pool = self.redis_pool.clone();
            let worker_key = worker_key_.clone();
            // report a worker has done a job
            continuation.map(move |job| {
                    let conn = pool.get().unwrap();
                    let _: Result<()> = conn.hdel(&worker_key, &WORKER_ID.with(|id| id.clone()))
                        .map_err(|err| err.into());
                    job
                })
                .boxed()
        } else {
            error!("unknown job class '{}'", agent.class);
            let errkind = UnknownJobClass(agent.class.clone());
            err((agent, errkind.into())).boxed()
        };

        for middleware in &mut self.middlewares {
            continuation = middleware.after(continuation).boxed();
        }

        // update failed / succeeded job count
        let proceeded_key_date =
            self.with_namespace(&format!("stat:processed:{}", UTC::now().format("%Y-%m-%d")));
        let proceeded_key = self.with_namespace(&format!("stat:processed"));
        let failed_key_date =
            self.with_namespace(&format!("stat:failed:{}", UTC::now().format("%Y-%m-%d")));
        let failed_key = self.with_namespace(&format!("stat:failed"));
        let pool = self.redis_pool.clone();
        continuation.then(move |result| {
                let connection = pool.get().unwrap();
                match result {
                        Ok(_) => {
                            Pipeline::new()
                                .incr(proceeded_key_date, 1)
                                .incr(proceeded_key, 1)
                                .query(&*connection)
                        }
                        Err(_) => {
                            Pipeline::new()
                                .incr(failed_key_date, 1)
                                .incr(failed_key, 1)
                                .query(&*connection)
                        }
                    }
                    .or_else(|err| Err(err.into()))
            })
            .boxed()
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
        Pipeline::new().hset_multiple(self.with_namespace(&self.identity()), &content)
            .expire(self.with_namespace(&self.identity()), 5)
            .sadd(self.with_namespace(&"processes"), self.identity())
            .query::<()>(&*conn)?;

        Ok(())

    }

    fn identity(&self) -> String {
        identity(self.pid, &self.rs)
    }


    fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }

    fn with_server_id(&self, snippet: &str) -> String {
        server_id(self.pid, &self.rs, snippet)
    }

    fn queue_name(&self, name: &str) -> String {
        self.with_namespace(&("queue:".to_string() + name))
    }
}

impl<'a> Drop for SidekiqServer<'a> {
    fn drop(&mut self) {
        info!("sidekiq-rs exiting");
    }
}

fn identity(pid: usize, rs: &str) -> String {
    let host = rust_gethostname().unwrap_or("unknown".into());
    let pid = pid;

    host + ":" + &pid.to_string() + ":" + rs
}

fn server_id(pid: usize, rs: &str, snippet: &str) -> String {
    identity(pid, rs) + ":" + snippet
}