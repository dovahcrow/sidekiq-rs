use random_choice::random_choice;
use server::{Signal, Operation};
use chan::{Sender, Receiver, tick};
use r2d2_redis::RedisConnectionManager;
use r2d2::Pool;
use job::Job;
use serde_json::from_str;
use errors::*;
use redis::Commands;
use std::collections::BTreeMap;
use job_handler::JobHandler;
use rand::Rng;
use json::parse;
use serde_json::to_string;
use chrono::UTC;

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use std::time::Duration;

pub struct SidekiqWorker {
    pub id: String,
    server_id: String,
    pool: Pool<RedisConnectionManager>,
    namespace: String,
    queues: Vec<String>,
    weights: Vec<f64>,
    handlers: BTreeMap<String, Box<JobHandler>>,
    tx: Sender<Signal>,
    rx: Receiver<Operation>,
    processed: usize,
    failed: usize,
}

impl SidekiqWorker {
    pub fn new(server_id: &str,
               pool: Pool<RedisConnectionManager>,
               tx: Sender<Signal>,
               rx: Receiver<Operation>,
               queues: Vec<String>,
               weights: Vec<f64>,
               handlers: BTreeMap<String, Box<JobHandler>>,
               namespace: String)
               -> SidekiqWorker {
        SidekiqWorker {
            id: ::rand::thread_rng().gen_ascii_chars().take(9).collect(),
            server_id: server_id.into(),
            pool: pool,
            namespace: namespace,
            queues: queues,
            weights: weights,
            handlers: handlers,
            tx: tx,
            rx: rx,
            processed: 0,
            failed: 0,
        }
    }

    pub fn work(mut self) {
        let mut choice = random_choice();
        info!("worker '{}' start working", self.with_server_id(&self.id));
        // main loop is here
        let rx = self.rx.clone();
        let clock = tick(Duration::from_secs(1));
        loop {
            chan_select! {
                default => {
                    let queue_name = {
                        let v = choice.random_choice_f64(&self.queues, &self.weights, 1);
                        v[0].clone()
                    };
                    match self.run_queue_once(&queue_name) {
                        Ok(true) => self.processed +=1,
                        Ok(false) => {}
                        Err(e) => {
                            self.failed += 1;
                            warn!("uncaught error '{}'", e);
                        }
                    };
                },
                clock.recv() => {
                    // synchronize state
                    if self.processed != 0 {
                        self.tx.send(Signal::Complete(self.id.clone(), self.processed));
                        self.processed = 0;
                    }
                    if self.failed != 0 {
                        self.tx.send(Signal::Fail(self.id.clone(), self.failed));
                        self.failed = 0;
                    }
                },
                rx.recv() -> op => {
                    if let Some(Operation::Terminate) = op {
                        info!("{}: Terminate signal received, exiting...", self.id);
                        self.tx.send(Signal::Terminated(self.id.clone()));
                        return;
                    } else {
                        unimplemented!()
                    }
                },
            }
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn run_queue_once(&mut self, name: &str) -> Result<bool> {
        let queue_name = self.queue_name(name);
        debug!("queue name '{}'", queue_name);

        let result: Option<Vec<String>> = try!(try!(self.pool.get()).brpop(&queue_name, 2));

        if let Some(result) = result {
            let job: Job = try!(from_str(&result[1]));
            self.tx.send(Signal::Acquire(self.id.clone()));
            try!(self.report_working(&job));
            try!(self.perform(&job));
            try!(self.report_done());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn perform(&mut self, job: &Job) -> Result<()> {
        debug!("job is {:?}", job);
        if let Some(handler) = self.handlers.get_mut(&job.class) {
            match catch_unwind(AssertUnwindSafe(|| handler.handle(&job))) {
                Err(e) => {
                    error!("Worker '{}' panicked", self.id);
                    self.tx.send(Signal::Terminated(self.id.clone()));
                    resume_unwind(e)
                }
                Ok(r) => {
                    try!(r);
                }
            }
            Ok(())
        } else {
            warn!("unknown job class '{}'", job.class);
            Ok(())
        }
    }

    // Sidekiq dashboard reporting functions

    #[cfg_attr(feature="flame_it", flame)]
    fn report_working(&self, job: &Job) -> Result<()> {
        let payload = object! {
            "queue" => job.queue.clone(),
            "payload" => parse(&to_string(job).unwrap()).unwrap(),
            "run_at" => UTC::now().timestamp()
        };
        try!(try!(self.pool.get()).hset(&self.with_namespace(&self.with_server_id("workers")),
                                        &self.id,
                                        payload.dump()));
        let _ = try!(try!(self.pool.get())
            .expire(&self.with_namespace(&self.with_server_id("workers")), 5));
        Ok(())
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn report_done(&self) -> Result<()> {
        let _ = try!(try!(self.pool.get())
            .hdel(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id));
        Ok(())
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn with_server_id(&self, snippet: &str) -> String {
        self.server_id.clone() + ":" + snippet
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn queue_name(&self, name: &str) -> String {
        self.with_namespace(&("queue:".to_string() + name))
    }
    // fn json_to_ruby_obj(v: &JValue) -> RObject {
    //     match v {x
    //         &JValue::Null => RNil::new().to_any_object(),
    //         &JValue::String(ref s) => RString::new(&s).to_any_object(),
    //         &JValue::U64(u) => RFixnum::new(u as i64).to_any_object(),
    //         &JValue::I64(i) => RFixnum::new(i as i64).to_any_object(),
    //         &JValue::Bool(b) => RBool::new(b).to_any_object(),
    //         &JValue::Array(ref arr) => {
    //             arr.iter()
    //                 .map(|ref v| Self::json_to_ruby_obj(v))
    //                 .collect::<RArray>()
    //                 .to_any_object()
    //         }
    //         &JValue::Object(ref h) => {
    //             h.iter()
    //                 .fold(RHash::new(), |mut acc, (k, v)| {
    //                     acc.store(RString::new(&k), Self::json_to_ruby_obj(v));
    //                     acc
    //                 })
    //                 .to_any_object()
    //         }
    //         _ => unimplemented!(), // unimplemented for float
    //     }
    // }
}
