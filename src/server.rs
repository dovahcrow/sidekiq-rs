use ::job_handler::JobHandlerFactory;

use std::collections::BTreeMap;

use ::errors::*;
use ::utils::rust_gethostname;

use chrono::UTC;
use r2d2::{Pool, Config};
use r2d2_redis::RedisConnectionManager;
use rand::Rng;
use threadpool::ThreadPool;
use worker::SidekiqWorker;
use redis::Commands;

use chan::{sync, after, Receiver, Sender};
use chan_signal::{Signal as SysSignal, notify};

use std::sync::{Arc, Barrier};

use std::time::Duration;

use thread_id;

#[derive(Debug)]
pub enum Signal {
    Complete(String, usize),
    Fail(String, usize),
    Acquire(String),
    Empty(String),
}

pub enum Operation {
    Run,
    Terminate,
}

pub struct SidekiqServer<'a> {
    redispool: Pool<RedisConnectionManager>,
    threadpool: ThreadPool,
    pub namespace: Option<String>,
    job_handler_factories: BTreeMap<String, &'a mut JobHandlerFactory>,
    queues: Vec<String>,
    weights: Vec<f64>,
    started_at: f64,
    busy: usize,
    rs: String,
    pid: usize,
    signal_chan: Receiver<SysSignal>,
    concurrency: usize,
}

impl<'a> SidekiqServer<'a> {
    pub fn new(redis: &str, concurrency: usize) -> Self {
        let signal = notify(&[SysSignal::INT, SysSignal::USR1]); // should be here to set proper signal mask to all threads
        let now = UTC::now();
        let config = Config::builder()
            .pool_size(concurrency as u32 + 1)
            .build();
        let manager = RedisConnectionManager::new(redis).unwrap();
        let pool = Pool::new(config, manager).unwrap();
        SidekiqServer {
            redispool: pool,
            threadpool: ThreadPool::new(concurrency),
            namespace: None,
            job_handler_factories: BTreeMap::new(),
            queues: vec![],
            weights: vec![],
            started_at: now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1000000f64,
            busy: 0,
            pid: thread_id::get(),
            concurrency: concurrency,
            signal_chan: signal,
            // random itentity
            rs: ::rand::thread_rng().gen_ascii_chars().take(12).collect(),
        }
    }

    pub fn new_queue(&mut self, name: &str, weight: f64) {
        self.queues.push(name.into());
        self.weights.push(weight);
    }

    pub fn attach_handler_factory(&mut self, name: &str, handle: &'a mut JobHandlerFactory) {
        self.job_handler_factories.insert(name.into(), handle);
    }


    pub fn start(&mut self) {
        info!("sidekiq-rs is running...");
        let (tsx, rsx) = sync(self.concurrency);
        let (tox, rox) = sync(self.concurrency);
        let barrier = Arc::new(Barrier::new(self.concurrency + 1));
        let signal = self.signal_chan.clone();

        // start worker threads
        for _ in 0..self.concurrency {
            let worker = SidekiqWorker::new(&self.identity(),
                                            self.redispool.clone(),
                                            tsx.clone(),
                                            rox.clone(),
                                            barrier.clone(),
                                            self.queues.clone(),
                                            self.weights.clone(),
                                            self.job_handler_factories
                                                .iter_mut()
                                                .map(|(k, v): (&String,
                                                               &mut &mut JobHandlerFactory)| {
                                                    (k.clone(), v.produce())
                                                })
                                                .collect(),
                                            self.namespace.clone());

            self.threadpool.execute(move || worker.work());
        }

        // controller loop
        let (tox2, rsx2) = (tox.clone(), rsx.clone());
        loop {
            let _ = self.report_alive();
            chan_select! {
                signal.recv() -> signal => {
                    match signal {
                        Some(SysSignal::USR1) => {
                            info!("{:?}: Terminating", signal.unwrap());
                            self.terminate_gracefully(tox2, rsx2, barrier);
                            break;
                        }
                        Some(SysSignal::INT) => {
                            info!("{:?}: Force terminating", signal.unwrap());                            
                            self.terminate_forcely(tox2, rsx2);
                            break;
                        }
                        Some(_) => {unimplemented!()}
                        None => {unimplemented!()}
                    }
                },
                tox.send(Operation::Run) => {},
                rsx.recv() -> sig => {
                    debug!("received signal {:?}", sig);
                    sig.map(|s| self.deal_signal(s));
                }
            }
        }

        // exiting
        info!("sidekiq exited");
    }

    fn inform_termination(&self, tox: Sender<Operation>) {
        for _ in 0..self.concurrency {
            tox.send(Operation::Terminate);
        }
    }

    fn terminate_forcely(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox);

        let timer = after(Duration::from_secs(10));
        // deplete the signal channel
        loop {
            chan_select! {
                timer.recv() => {
                    info!("force quitting");
                    break;
                },
                rsx.recv() -> sig => {
                    debug!("received signal {:?}", sig);
                    sig.map(|s| self.deal_signal(s));
                },
            }
        }
    }

    fn terminate_gracefully(&mut self,
                            tox: Sender<Operation>,
                            rsx: Receiver<Signal>,
                            barrier: Arc<Barrier>) {
        self.inform_termination(tox);

        // deplete the signal channel
        loop {
            chan_select! {
                default => {
                    break;
                },
                rsx.recv() -> sig => {
                    debug!("received signal {:?}", sig);
                    sig.map(|s| self.deal_signal(s));
                },
            }
        }

        info!("waiting for other workers exit");
        barrier.wait();
    }

    fn deal_signal(&mut self, sig: Signal) {
        match sig {
            Signal::Complete(_, n) => {
                let _ = self.report_processed(n);
                if self.busy != 0 {
                    self.busy -= 1;
                }
            }
            Signal::Fail(_, n) => {
                let _ = self.report_failed(n);
                if self.busy != 0 {
                    self.busy -= 1;
                }
            }
            Signal::Empty(_) => {}
            Signal::Acquire(_) => {
                self.busy += 1;
            }
        }
    }

    fn report_alive(&mut self) -> Result<()> {
        let now = UTC::now();
        let content = vec![("info",
                            object! {
                                "hostname"=> rust_gethostname().unwrap_or("unknown".into()),
                                "started_at"=> self.started_at,
                                "pid"=> self.pid,
                                "concurrency"=> self.concurrency,
                                "queues"=> self.queues.clone(),
                                "labels"=> array![],
                                "identity"=> self.identity()
                            }
                               .dump()),
                           ("busy", self.busy.to_string()),
                           ("beat",
                            (now.timestamp() as f64 +
                             now.timestamp_subsec_micros() as f64 / 1000000f64)
                               .to_string())];

        let _ = self.redispool
            .get()
            .unwrap()
            .hset_multiple(self.with_namespace(&self.identity()), &content)?;
        let _ = self.redispool.get().unwrap().expire(self.with_namespace(&self.identity()), 5)?;
        let _ =
            self.redispool.get().unwrap().sadd(self.with_namespace(&"processes"), self.identity())?;
        Ok(())

    }

    fn report_processed(&mut self, n: usize) -> Result<()> {
        let key = self.with_namespace(&format!("stat:processed:{}", UTC::now().format("%Y-%m-%d")));
        let connection = self.redispool.get().unwrap();
        let _ = connection.incr(key, n)?;

        let key = self.with_namespace(&format!("stat:processed"));
        let connection = self.redispool.get().unwrap();
        let _ = connection.incr(key, n)?;
        Ok(())
    }

    fn report_failed(&mut self, n: usize) -> Result<()> {
        let key = self.with_namespace(&format!("stat:failed:{}", UTC::now().format("%Y-%m-%d")));
        let connection = self.redispool.get().unwrap();
        let _ = connection.incr(key, n)?;

        let key = self.with_namespace(&format!("stat:failed"));
        let connection = self.redispool.get().unwrap();
        let _ = connection.incr(key, n)?;
        Ok(())
    }

    fn identity(&self) -> String {
        let host = rust_gethostname().unwrap_or("unknown".into());
        let pid = self.pid;

        host + ":" + &pid.to_string() + ":" + &self.rs
    }

    fn with_namespace(&self, snippet: &str) -> String {
        self.namespace.clone().map(|v| v + ":").unwrap_or("".into()) + snippet
    }
}
