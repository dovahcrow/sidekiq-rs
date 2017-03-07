
use std::panic::{catch_unwind, AssertUnwindSafe};

use std::time::Duration;
use std::collections::BTreeMap;

use random_choice::random_choice;

use chan::{Sender, Receiver, tick};

use serde_json::from_str;
use errors::*;
use redis::{Commands, PipelineCommands, Pipeline};


use rand::Rng;
use serde_json::{to_string, Value as JValue};
use chrono::UTC;

use server::{Signal, Operation};
use job::Job;
use job_handler::{JobHandler, JobHandlerResult};
use middleware::MiddleWare;
use RedisPool;
use JobSuccessType;


pub struct SidekiqWorker<'a> {
    pub id: String,
    server_id: String,
    pool: RedisPool,
    namespace: String,
    queues: Vec<String>,
    weights: Vec<f64>,
    handlers: BTreeMap<String, Box<JobHandler + 'a>>,
    middlewares: Vec<Box<MiddleWare + 'a>>,
    tx: Sender<Signal>,
    rx: Receiver<Operation>,
    processed: usize,
    failed: usize,
}

impl<'a> SidekiqWorker<'a> {
    pub fn new(server_id: &str,
               pool: RedisPool,
               tx: Sender<Signal>,
               rx: Receiver<Operation>,
               queues: Vec<String>,
               weights: Vec<f64>,
               handlers: BTreeMap<String, Box<JobHandler>>,
               middlewares: Vec<Box<MiddleWare>>,
               namespace: String)
               -> SidekiqWorker<'a> {
        SidekiqWorker {
            id: ::rand::thread_rng().gen_ascii_chars().take(9).collect(),
            server_id: server_id.into(),
            pool: pool,
            namespace: namespace,
            queues: queues,
            weights: weights,
            handlers: handlers,
            middlewares: middlewares,
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
                    debug!("{} run queue once", self.id);
                    match self.run_queue_once(&queue_name) {
                        Ok(true) => self.processed += 1,
                        Ok(false) => {}
                        Err(e) => {
                            self.failed += 1;
                            warn!("uncaught error '{}'", e);
                        }
                    };
                },
                clock.recv() => {
                    // synchronize state
                    debug!("{} syncing state", self.id);
                    self.sync_state();
                    debug!("{} syncing state done", self.id);
                },
                rx.recv() -> op => {
                    if let Some(Operation::Terminate) = op {
                        info!("{}: Terminate signal received, exiting...", self.id);
                        self.tx.send(Signal::Terminated(self.id.clone()));
                        debug!("{}: Terminate signal sent", self.id);
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
        debug!("{}: queue name '{}'", self.id, queue_name);

        let result: Option<Vec<String>> = try!(try!(self.pool.get()).brpop(&queue_name, 2));

        if let Some(result) = result {
            let mut job: Job = try!(from_str(&result[1]));
            self.tx.send(Signal::Acquire(self.id.clone()));
            job.namespace = self.namespace.clone();
            try!(self.report_working(&job));
            let r = try!(self.perform(job));
            try!(self.report_done());
            match r {
                JobSuccessType::Ignore => Ok(false),
                JobSuccessType::Success => Ok(true),
            }
        } else {
            Ok(false)
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn perform(&mut self, job: Job) -> Result<JobSuccessType> {
        debug!("{}: job is {:?}", self.id, job);

        let mut handler = if let Some(handler) = self.handlers.get_mut(&job.class) {
            handler.cloned()
        } else {
            warn!("unknown job class '{}'", job.class);
            return Err("unknown job class".into());
        };

        match catch_unwind(AssertUnwindSafe(|| { self.call_middleware(job, |job| handler.handle(job)) })) {
            Err(_) => {
                error!("Worker '{}' panicked, recovering", self.id);
                Err("Worker crashed".into())
            }
            Ok(r) => Ok(try!(r)),
        }
    }

    fn call_middleware<F>(&mut self, mut job: Job, mut job_handle: F) -> Result<JobSuccessType>
        where F: FnMut(&Job) -> JobHandlerResult
    {
        fn imp<'a, F: FnMut(&Job) -> JobHandlerResult>(job: &mut Job,
                                                       redis: RedisPool,
                                                       chain: &mut [Box<MiddleWare + 'a>],
                                                       job_handle: &mut F)
                                                       -> Result<JobSuccessType> {
            chain.split_first_mut()
                .map(|(head, tail)| {
                    head.handle(job,
                                redis,
                                &mut |job, redis| imp(job, redis, tail, job_handle))
                })
                .or_else(|| Some(job_handle(&job)))
                .unwrap()
        }

        imp(&mut job,
            self.pool.clone(),
            &mut self.middlewares,
            &mut job_handle)
    }

    fn sync_state(&mut self) {
        if self.processed != 0 {
            debug!("{} sending complete signal", self.id);
            self.tx.send(Signal::Complete(self.id.clone(), self.processed));
            self.processed = 0;
        }
        if self.failed != 0 {
            debug!("{} sending fail signal", self.id);
            self.tx.send(Signal::Fail(self.id.clone(), self.failed));
            self.failed = 0;
        }
    }

    // Sidekiq dashboard reporting functions

    #[cfg_attr(feature="flame_it", flame)]
    fn report_working(&self, job: &Job) -> Result<()> {
        let conn = try!(self.pool.get());
        let payload: JValue = json!({
            "queue": job.queue.clone(),
            "payload": job,
            "run_at": UTC::now().timestamp()
        });
        let _: () = Pipeline::new().hset(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id,
                  to_string(&payload).unwrap())
            .expire(&self.with_namespace(&self.with_server_id("workers")), 5)
            .query(&*conn)?;

        Ok(())
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn report_done(&self) -> Result<()> {
        let _: () = self.pool
            .get()?
            .hdel(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id)?;
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
