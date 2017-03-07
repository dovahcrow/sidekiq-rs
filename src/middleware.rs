use serde_json::to_string;
use chrono::UTC;

use RedisPool;
use JobSuccessType;
use errors::Result;
use job::Job;

pub type MiddleWareResult = Result<JobSuccessType>;
pub type NextFunc<'a> = &'a mut (FnMut(&mut Job, RedisPool) -> MiddleWareResult + 'a);

pub trait MiddleWare: Send {
    fn handle(&mut self, job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult;
    fn cloned(&mut self) -> Box<MiddleWare>;
}

impl<F> MiddleWare for F
    where F: FnMut(&mut Job, RedisPool, NextFunc) -> MiddleWareResult + Copy + Send + 'static
{
    fn handle(&mut self, job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult {
        self(job, redis, next)
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(*self)
    }
}

pub fn peek_middleware(job: &mut Job, redis: RedisPool, mut next: NextFunc) -> MiddleWareResult {
    println!("Before Call {:?}", job);
    let r = next(job, redis);
    println!("After Call {:?}", job);
    r
}

pub fn retry_middleware(job: &mut Job, redis: RedisPool, mut next: NextFunc) -> MiddleWareResult {
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
                    let _: () = conn.lpush(job.queue_name(), to_string(job).unwrap())?;
                    Ok(JobSuccessType::Ignore)
                }
                USize(u) if u > 0 => {
                    warn!("'{:?}' failed with '{}', retrying", job, e);
                    job.retry = USize(u - 1);
                    let _: () = conn.lpush(job.queue_name(), to_string(job).unwrap())?;
                    Ok(JobSuccessType::Ignore)
                }
                _ => Err(e),
            }
        }
        Ok(o) => Ok(o),
    }
}

pub fn time_elapse_middleware(job: &mut Job,
                              redis: RedisPool,
                              mut next: NextFunc)
                              -> MiddleWareResult {
    let j = job.clone();
    let now = UTC::now();
    let r = next(job, redis);
    let that = UTC::now();
    info!("'{:?}' takes {}", j, that.signed_duration_since(now));
    r
}