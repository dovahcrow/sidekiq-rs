use std::collections::BTreeMap;
use std::time::Duration;

use redis::{Pipeline, PipelineCommands};
use r2d2::{Pool, Config};
use r2d2_redis::RedisConnectionManager;

use rand::Rng;

use threadpool::ThreadPool;

use chan::{sync, after, tick, Receiver, Sender};
use chan_signal::{Signal as SysSignal, notify};

use libc::getpid;

use chrono::UTC;

use serde_json::to_string;

use worker::SidekiqWorker;
use errors::*;
use utils::rust_gethostname;
use middleware::MiddleWare;
use job_handler::JobHandler;
use RedisPool;

#[derive(Debug)]
pub enum Signal {
    Complete(String, usize),
    Fail(String, usize),
    Acquire(String),
    Terminated(String),
}

pub enum Operation {
    Terminate,
}

pub struct SidekiqServer<'a> {
    redispool: RedisPool,
    threadpool: ThreadPool,
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
    pub force_quite_timeout: usize,
}

impl<'a> SidekiqServer<'a> {
    // Interfaces to be exposed

    pub fn new(redis: &str, concurrency: usize) -> Result<Self> {
        let signal = notify(&[SysSignal::INT, SysSignal::USR1]); // should be here to set proper signal mask to all threads
        let now = UTC::now();
        let config = Config::builder()
            .pool_size(concurrency as u32 + 3) // dunno why, it corrupt for unable to get connection sometimes with concurrency + 1
            .build();
        let manager = try!(RedisConnectionManager::new(redis));
        let pool = try!(Pool::new(config, manager));
        Ok(SidekiqServer {
            redispool: pool,
            threadpool: ThreadPool::new_with_name("worker".into(), concurrency),
            namespace: String::new(),
            job_handlers: BTreeMap::new(),
            queues: vec![],
            weights: vec![],
            started_at: now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1000000f64,
            pid: unsafe { getpid() } as usize,
            worker_info: BTreeMap::new(),
            concurrency: concurrency,
            signal_chan: signal,
            force_quite_timeout: 10,
            middlewares: vec![],
            // random itentity
            rs: ::rand::thread_rng().gen_ascii_chars().take(12).collect(),
        })
    }

    pub fn new_queue(&mut self, name: &str, weight: usize) {
        self.queues.push(name.into());
        self.weights.push(weight as f64);
    }

    pub fn attach_handler<T: JobHandler + 'a>(&mut self, name: &str, handle: T) {
        self.job_handlers.insert(name.into(), Box::new(handle));
    }

    pub fn attach_middleware<T: MiddleWare + 'a>(&mut self, factory: T) {
        self.middlewares.push(Box::new(factory));
    }

    pub fn start(&mut self) {
        info!("sidekiq-rs is running...");
        if self.queues.len() == 0 {
            error!("queue is empty, exiting");
            return;
        }
        let (tsx, rsx) = sync(self.concurrency + 10);
        let (tox, rox) = sync(self.concurrency + 10);
        let signal = self.signal_chan.clone();

        // start worker threads
        self.launch_workers(tsx.clone(), rox.clone());

        // controller loop
        let (tox2, rsx2) = (tox.clone(), rsx.clone()); // rename channels cuz `chan_select!` will rename'em below
        let clock = tick(Duration::from_secs(2)); // report to sidekiq every 5 secs
        loop {
            if let Err(e) = self.report_alive() {
                error!("report alive failed: '{}'", e);
            }
            chan_select! {
                signal.recv() -> signal => {
                    match signal {
                        Some(signal @ SysSignal::USR1) => {
                            info!("{:?}: Terminating", signal);
                            self.terminate_gracefully(tox2, rsx2);
                            break;
                        }
                        Some(signal @ SysSignal::INT) => {
                            info!("{:?}: Force terminating", signal);                            
                            self.terminate_forcely(tox2, rsx2);
                            break;
                        }
                        Some(_) => { unimplemented!() }
                        None => { unimplemented!() }
                    }
                },
                clock.recv() => {
                    debug!("server clock triggered");
                },
                rsx.recv() -> sig => {
                    debug!("received signal {:?}", sig);
                    if let Some(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                        error!("error when dealing signal: '{}'", e);
                    }
                    let worker_count = self.threadpool.active_count();
                    // relaunch workers if they died unexpectly
                    if worker_count < self.concurrency {
                        warn!("worker down, restarting");
                        self.launch_workers(tsx.clone(), rox.clone());
                    } else if worker_count > self.concurrency {
                        unreachable!("unreachable! worker_count can never larger than concurrency!")
                    }
                }
            }
        }

        // exiting
        info!("sidekiq exited");
    }

    // Worker start/terminate functions


    fn launch_workers(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
        while self.worker_info.len() < self.concurrency {
            self.launch_worker(tsx.clone(), rox.clone());
        }
    }


    fn launch_worker(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
        let worker = SidekiqWorker::new(&self.identity(),
                                        self.redispool.clone(),
                                        tsx,
                                        rox,
                                        self.queues.clone(),
                                        self.weights.clone(),
                                        self.job_handlers
                                            .iter_mut()
                                            .map(|(k, v)| (k.clone(), v.cloned()))
                                            .collect(),
                                        self.middlewares.iter_mut().map(|v| v.cloned()).collect(),
                                        self.namespace.clone());
        self.worker_info.insert(worker.id.clone(), false);
        self.threadpool.execute(move || worker.work());
    }

    fn inform_termination(&self, tox: Sender<Operation>) {
        for _ in 0..self.concurrency {
            tox.send(Operation::Terminate);
        }
    }

    fn terminate_forcely(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox);

        let timer = after(Duration::from_secs(self.force_quite_timeout as u64));
        // deplete the signal channel
        loop {
            chan_select! {
                timer.recv() => {
                    info!("force quitting");
                    break
                },
                rsx.recv() -> sig => {
                    debug!("received signal {:?}", sig);
                    if let Some(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                        error!("error when dealing signal: '{}'", e);
                    }
                    if self.worker_info.len() == 0 {
                        break
                    }
                },
            }
        }
    }


    fn terminate_gracefully(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox);

        info!("waiting for other workers exit");
        // deplete the signal channel
        loop {
            chan_select! {
                rsx.recv() -> sig => {
                    debug!("received signal {:?}", sig);
                    if let Some(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                        error!("error when dealing signal: '{}'", e);
                    }
                    if self.worker_info.len()== 0 {
                        break
                    }
                },
            }
        }
    }


    fn deal_signal(&mut self, sig: Signal) -> Result<()> {
        debug!("dealing signal {:?}", sig);
        match sig {
            Signal::Complete(id, n) => {
                let _ = try!(self.report_processed(n));
                *self.worker_info.get_mut(&id).unwrap() = false;
            }
            Signal::Fail(id, n) => {
                let _ = try!(self.report_failed(n));
                *self.worker_info.get_mut(&id).unwrap() = false;
            }
            Signal::Acquire(id) => {
                self.worker_info.insert(id, true);
            }
            Signal::Terminated(id) => {
                self.worker_info.remove(&id);
            }
        }
        debug!("signal dealt");
        Ok(())
    }

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
        let conn = try!(self.redispool.get());
        try!(Pipeline::new()
            .hset_multiple(self.with_namespace(&self.identity()), &content)
            .expire(self.with_namespace(&self.identity()), 5)
            .sadd(self.with_namespace(&"processes"), self.identity())
            .query::<()>(&*conn));

        Ok(())

    }


    fn report_processed(&mut self, n: usize) -> Result<()> {
        let connection = try!(self.redispool.get());
        let _: () = Pipeline::new().incr(self.with_namespace(&format!("stat:processed:{}",
                                               UTC::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:processed")), n)
            .query(&*connection)?;

        Ok(())
    }


    fn report_failed(&mut self, n: usize) -> Result<()> {
        let connection = try!(self.redispool.get());
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
}
