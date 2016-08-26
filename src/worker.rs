use random_choice::random_choice;
use server::{Signal, Operation};
use chan::{Sender, Receiver};
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

pub struct SidekiqWorker {
    id: String,
    server_id: String,
    pool: Pool<RedisConnectionManager>,
    namespace: Option<String>,
    queues: Vec<String>,
    weights: Vec<f64>,
    handlers: BTreeMap<String, Box<JobHandler>>,
    tx: Sender<Signal>,
    rx: Receiver<Operation>,
}

impl SidekiqWorker {
    pub fn new(server_id: &str,
               pool: Pool<RedisConnectionManager>,
               tx: Sender<Signal>,
               rx: Receiver<Operation>,
               queues: Vec<String>,
               weights: Vec<f64>,
               handlers: BTreeMap<String, Box<JobHandler>>,
               namespace: Option<String>)
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
        }
    }

    pub fn work(mut self) {
        let mut choice = random_choice();
        loop {
            match self.rx.recv() {
                Some(Operation::Run) => {
                    let queue_name = {
                        let v = choice.random_choice_f64(&self.queues, &self.weights, 1);
                        v[0].clone()
                    };
                    match self.run_queue_once(&queue_name) {
                        Ok(true) => self.tx.send(Signal::Complete(self.id.clone(), 1)),
                        Ok(false) => self.tx.send(Signal::Empty(self.id.clone())),
                        Err(e) => {
                            self.tx.send(Signal::Fail(self.id.clone(), 1));
                            warn!("uncaught error '{}'", e);
                        }
                    };
                }
                Some(Operation::Terminate) => {
                    info!("{}: Terminate signal received, exit...", self.id);
                    return;
                }
                None => unimplemented!(),
            }
        }
    }

    fn run_queue_once(&mut self, name: &str) -> Result<bool> {
        let queue_name = self.queue_name(name);
        debug!("queue name '{}'", queue_name);

        let result: Option<Vec<String>> = self.pool.get()?.brpop(&queue_name, 2)?;

        if let Some(result) = result {
            let job: Job = from_str(&result[1])?;
            self.tx.send(Signal::Acquire(self.id.clone()));
            self.report_working(&job)?;
            self.perform(&job)?;
            self.report_done()?;
            Ok(true)
        } else {
            Ok(false)
        }

    }

    fn perform(&mut self, job: &Job) -> Result<()> {
        debug!("job is {:?}", job);
        if let Some(handler) = self.handlers.get_mut(&job.class) {
            handler.handle(&job)?;
            Ok(())
        } else {
            warn!("unknown job class '{}'", job.class);
            Ok(())
        }
    }

    fn report_working(&self, job: &Job) -> Result<()> {
        let payload = object! {
            "queue" => job.queue.clone(),
            "payload" => parse(&to_string(job).unwrap()).unwrap(),
            "run_at" => UTC::now().timestamp()
        };
        self.pool
            .get()?
            .hset(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id,
                  payload.dump())?;
        let _ = self.pool.get()?.expire(&self.with_namespace(&self.with_server_id("workers")), 5)?;
        Ok(())
    }

    fn report_done(&self) -> Result<()> {
        let _ = self.pool
            .get()?
            .hdel(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id)?;
        Ok(())
    }

    fn with_namespace(&self, snippet: &str) -> String {
        self.namespace.clone().map(|v| v + ":").unwrap_or("".into()) + snippet
    }

    fn with_server_id(&self, snippet: &str) -> String {
        self.server_id.clone() + ":" + snippet
    }

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
