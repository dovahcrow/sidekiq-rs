use std::ops::{Deref, DerefMut};
use std::convert::Into;

use serde_json::to_string;
use redis::Commands;

use job::Job;
use errors::Result;
use RedisPool;
use errors::ErrorKind::*;


#[derive(Constructor, Debug, Clone)]
pub struct JobAgent {
    job: Job,
    redis: RedisPool,
}


impl Deref for JobAgent {
    type Target = Job;
    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl Into<Job> for JobAgent {
    fn into(self) -> Job {
        self.job
    }
}

impl DerefMut for JobAgent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.job
    }
}

impl JobAgent {
    pub fn put_back_retry(&self) -> Result<()> {
        let conn = self.redis.get()?;
        let payload = to_string(&self.job)?;
        let score = self.job.retry_info.as_ref().ok_or(NoRetryInfo)?.retried_at.timestamp();
        let _: () = conn.zadd(self.job.namespace.clone() + ":retry", payload, score)?;
        Ok(())
    }
}
