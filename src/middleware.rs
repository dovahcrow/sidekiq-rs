use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;

use serde_json::to_string;
use chrono::{UTC, DateTime};
use futures::{BoxFuture, Future, IntoFuture};
use futures::future::{ok, err};


use RedisPool;
use errors::{Result, Error};
use job_agent::JobAgent;
use job::{Job, RetryInfo};
use FutureJob;

pub trait MiddleWare: Send {
    fn before(&mut self, continuation: FutureJob) -> FutureJob {
        continuation
    }
    fn cloned(&mut self) -> Box<MiddleWare>;
    fn after(&mut self, continuation: FutureJob) -> FutureJob {
        continuation
    }
}

mod peek_middleware {
    use super::JobAgent;
    fn peek_before(job: JobAgent) -> JobAgent {
        println!("Before Call {:?}", *job);
        job
    }
    fn peek_after(job: JobAgent) -> JobAgent {
        println!("After Call {:?}", *job);
        job
    }
    pub static PeekMiddleware: (fn(JobAgent) -> JobAgent, fn(JobAgent) -> JobAgent) = (peek_before,
                                                                                       peek_after);
}

pub use self::peek_middleware::PeekMiddleware;

#[derive(Copy, Clone)]
pub struct RetryMiddleware;
impl MiddleWare for RetryMiddleware {
    fn after(&mut self, continuation: FutureJob) -> FutureJob {
        use job::BoolOrUSize::{self, Bool, USize};

        continuation.or_else(|(mut job, e)| {
                let retry_count = job.retry_info.as_ref().map(|i| i.retry_count).unwrap_or(0);
                match (&job.retry, usize::max_value()) {
                    (&Bool(true), u) | (&USize(u), _) if retry_count < u => {
                        warn!("Job '{:?}' failed with '{}', retrying", job, e);
                        job.retry_info = Some(RetryInfo {
                            retry_count: retry_count + 1,
                            error_message: format!("{}", e),
                            error_class: "dummy".to_string(),
                            error_backtrace: e.backtrace()
                                .map(|bt| {
                                    let s = format!("{:?}", bt);
                                    s.split('\n').map(|s| s.to_string()).collect()
                                })
                                .unwrap_or(vec![]),
                            failed_at: UTC::now(),
                            retried_at: None,
                        });
                        let result = job.put_back_retry();

                        result.map(|_| job.clone())
                            .map_err(|e| (job, e))
                            .into_future()
                    }
                    _ => err((job, e)),
                }
            })
            .boxed()
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(*self)
    }
}

#[derive(Clone)]
pub struct TimeElapseMiddleware {
    time_slots: Arc<Mutex<HashMap<String, DateTime<UTC>>>>,
}

impl TimeElapseMiddleware {
    pub fn new() -> TimeElapseMiddleware {
        TimeElapseMiddleware { time_slots: Arc::new(Mutex::new(HashMap::new())) }
    }
    fn record_start(&mut self, id: &str) {
        self.time_slots.lock().unwrap().insert(id.to_string(), UTC::now());
    }

    fn record_end(&mut self, id: &str) -> Option<DateTime<UTC>> {
        self.time_slots.lock().unwrap().remove(id)
    }
}

impl MiddleWare for TimeElapseMiddleware {
    fn before(&mut self, continuation: FutureJob) -> FutureJob {
        let mut ego = self.clone();
        continuation.map(move |job| {
                ego.record_start(&job.class);
                job
            })
            .boxed()
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(Self::new())
    }
    fn after(&mut self, continuation: FutureJob) -> FutureJob {
        let mut ego = self.clone();
        continuation.map(move |job| {
                if let Some(that) = ego.record_end(&job.class) {
                    let now = UTC::now();
                    info!("'{:?}' takes {}", job, that.signed_duration_since(now));
                }
                job
            })
            .boxed()
    }
}

macro_rules! fn_impl {
    ({$($t1: tt)*} {$($t2: tt)*} ) => (
        impl MiddleWare for ($($t1)*, $($t2)*) {
            fn before(&mut self, continuation: FutureJob) -> FutureJob {
                let ego = self.0.clone();
                fn_impl_body!($($t1)*, continuation, ego)
            }
            fn cloned(&mut self) -> Box<MiddleWare> {
                Box::new(*self)
            }
            fn after(&mut self, continuation: FutureJob) -> FutureJob {
                let ego = self.1.clone();
                fn_impl_body!($($t2)*, continuation, ego)
            }
        }
    );
    ({ $($t: tt)* } $($lf: tt),*) => (
        impl<$($lf),*> MiddleWare for $($t)* {
            fn before(&mut self, continuation: FutureJob) -> FutureJob {
                let ego = self.clone();
                fn_impl_body!($($t)*, continuation, ego)
            }
            fn cloned(&mut self) -> Box<MiddleWare> {
                Box::new(*self)
            }
        }
    );
}

macro_rules! fn_impl_body {
    (fn(&'a mut JobAgent), $continuation: expr, $ego: expr) => (
        $continuation.map(move |job| {
            $ego(&mut job);
            job
        }).boxed()
    );
    (fn(&'a JobAgent), $continuation: expr, $ego: expr) => (
        $continuation.map(move |job| {
            $ego(&job);
            job
        }).boxed()
    );
    (fn(JobAgent) -> JobAgent, $continuation: expr, $ego: expr) => (
        $continuation.and_then(move |job| ok($ego(job))).boxed()
    );
    (fn(JobAgent) -> FutureJob, $continuation: expr, $ego: expr) => (
        $continuation.and_then(move |job| {
            $ego(job)
        }).boxed()
    );
    (fn(FutureJob) -> FutureJob, $continuation: expr, $ego: expr) => (
        $ego($continuation)
    )
}

macro_rules! permutation2 {
    ($callback: ident => { $($a: tt)* }) => {
        $callback!({ $($a)* } { $($a)* });
    };
    ($callback: ident => { $($a: tt)* } { $($b: tt)* }) => {
        $callback!({ $($a)* } { $($b)* });
        $callback!({ $($b)* } { $($a)* });
    };
    ($callback: ident => { $($a: tt)* } { $($b: tt)* } $($t: tt)*) => {
        permutation2!($callback => { $($a)* });
        permutation2!($callback => { $($a)* } { $($b)* });
        permutation2!($callback => { $($a)* } $($t)* );
        permutation2!($callback => { $($b)* } $($t)* );
    }
}

// fn_impl! { { fn(&'a JobAgent) }}
// fn_impl! { { fn(&'a mut JobAgent) }}
fn_impl! { { fn(JobAgent) -> JobAgent } }
fn_impl! { { fn(JobAgent) -> FutureJob } }
fn_impl! { { fn(FutureJob) -> FutureJob } }

permutation2!{ fn_impl =>
            //    { fn(&'a JobAgent) } 
            //    { fn(&'a mut JobAgent) } 
               { fn(JobAgent) -> JobAgent } 
               { fn(JobAgent) -> FutureJob } 
            //    { fn(FutureJob) -> FutureJob } 
}