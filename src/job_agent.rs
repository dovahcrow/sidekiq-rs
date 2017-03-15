use std::ops::{Deref, DerefMut};
use std::convert::Into;

use serde_json::to_string;
use redis::{Commands, Pipeline, PipelineCommands};
use chrono::{UTC, Duration};

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

        let _: () = conn.zadd(self.job.with_namespace("retry"), payload, score)?;
        Ok(())
    }

    pub fn put_to_morgue(&self) -> Result<()> {
        let conn = self.redis.get()?;
        let payload = to_string(&self.job)?;
        let score = self.job.retry_info.as_ref().ok_or(NoRetryInfo)?.retried_at.timestamp();
        let _: () = Pipeline::new().zadd(self.job.with_namespace("dead"), payload, score)
            .zrembyscore("dead", 0, (UTC::now() - Duration::days(30)).timestamp())
            .zrembyrank("dead", 0, -10000)
            .query(&*conn)?;
        Ok(())
    }
}
