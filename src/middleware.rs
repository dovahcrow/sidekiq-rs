use serde_json::to_string;
use chrono::UTC;

use ::RedisPool;
use ::JobSuccessType;
use errors::Result;
use job::Job;

pub type MiddleWareResult = Result<JobSuccessType>;
pub type NextFunc<'a> = &'a mut (FnMut(&mut Job, RedisPool) -> MiddleWareResult + 'a);

pub trait MiddleWare: Send {
    fn handle<'a>(&self, job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult;
    fn cloned(&mut self) -> Box<MiddleWare>;
}


pub struct PeekMiddleWare;

impl MiddleWare for PeekMiddleWare {
    fn handle<'a>(&self, job: &mut Job, redis: RedisPool, mut next: NextFunc) -> MiddleWareResult {
        println!("Before Call {:?}", job);
        let r = next(job, redis);
        println!("After Call {:?}", job);
        r
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(PeekMiddleWare)
    }
}

pub struct RetryMiddleWare;

impl MiddleWare for RetryMiddleWare {
    fn handle<'a>(&self, job: &mut Job, redis: RedisPool, mut next: NextFunc) -> MiddleWareResult {
        use redis::Commands;
        use job::BoolOrUSize::*;
        let conn = redis.get().unwrap();
        let r = next(job, redis);
        match r {
            Err(e) => {
                match job.retry {
                    Bool(true) => {
                        warn!("Job '{:?}' failed with '{}', retrying", job, e);
                        job.retry = Bool(false);
                        try!(conn.lpush(job.queue_name(), to_string(job).unwrap()));
                        Ok(JobSuccessType::Ignore)
                    }
                    USize(u) if u > 0 => {
                        warn!("'{:?}' failed with '{}', retrying", job, e);
                        job.retry = USize(u - 1);
                        try!(conn.lpush(job.queue_name(), to_string(job).unwrap()));
                        Ok(JobSuccessType::Ignore)
                    }
                    _ => Err(e),
                }
            }
            Ok(o) => Ok(o),
        }
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(RetryMiddleWare)
    }
}

pub struct TimeElapseMiddleWare;

impl MiddleWare for TimeElapseMiddleWare {
    fn handle<'a>(&self, job: &mut Job, redis: RedisPool, mut next: NextFunc) -> MiddleWareResult {
        let j = job.clone();
        let now = UTC::now();
        let r = next(job, redis);
        let that = UTC::now();
        info!("'{:?}' takes {}", j, that - now);
        r
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(TimeElapseMiddleWare)
    }
}