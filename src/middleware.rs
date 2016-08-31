use job::Job;
use r2d2::Pool;
use r2d2_redis::RedisConnectionManager;
use errors::Result;
use serde_json::to_string;

pub type MiddleWareResult = Result<()>;

pub trait MiddleWare: Send {
    fn handle<'a>(&self,
                  job: &mut Job,
                  redis: Pool<RedisConnectionManager>,
                  cb: &mut (FnMut(&mut Job, Pool<RedisConnectionManager>) -> MiddleWareResult + 'a))
                  -> MiddleWareResult;
    fn cloned(&mut self) -> Box<MiddleWare>;
}


pub struct PeekMiddleWare;

impl MiddleWare for PeekMiddleWare {
    fn handle<'a>(&self,
                  job: &mut Job,
                  redis: Pool<RedisConnectionManager>,
                  mut cb: &mut (FnMut(&mut Job, Pool<RedisConnectionManager>) -> MiddleWareResult + 'a))
                  -> MiddleWareResult {
        println!("Before Call {:?}", job);
        let r = cb(job, redis);
        println!("After Call {:?}", job);
        r
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(PeekMiddleWare)
    }
}

pub struct RetryMiddleWare;

impl MiddleWare for RetryMiddleWare {
    fn handle<'a>(&self,
                  job: &mut Job,
                  redis: Pool<RedisConnectionManager>,
                  mut cb: &mut (FnMut(&mut Job, Pool<RedisConnectionManager>) -> MiddleWareResult + 'a))
                  -> MiddleWareResult {
        use redis::Commands;
        use job::BoolOrUSize::*;
        let conn = redis.get().unwrap();
        let r = cb(job, redis);
        if r.is_err() {
            match job.retry {
                Bool(true) => {
                    warn!("Job '{:?}' failed with '{}', retrying",
                          job,
                          r.as_ref().unwrap_err());
                    job.retry = Bool(false);
                    try!(conn.lpush(job.queue_name(), to_string(job).unwrap()));
                    Ok(())
                }
                USize(u) if u > 0 => {
                    warn!("Job '{:?}' failed with '{}', retrying",
                          job,
                          r.as_ref().unwrap_err());
                    job.retry = USize(u - 1);
                    try!(conn.lpush(job.queue_name(), to_string(job).unwrap()));
                    Ok(())
                }
                _ => r,
            }
        } else {
            r
        }
    }
    fn cloned(&mut self) -> Box<MiddleWare> {
        Box::new(PeekMiddleWare)
    }
}