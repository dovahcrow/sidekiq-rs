use job::Job;
use r2d2::Pool;
use r2d2_redis::RedisConnectionManager;
use errors::Result;

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