use std::ops::{Deref, DerefMut};
use std::convert::Into;
use std::collections::HashMap;
use std::any::Any;
use job::Job;
use errors::Result;

#[derive(Constructor, Debug, Clone)]
pub struct JobAgent {
    job: Job,
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
        Ok(())
    }
}
