use ::job::Job;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::thread;
use std::time::Duration;

use std::marker::Sync;


impl Error for JobHandlerError {
    fn description(&self) -> &str {
        &self.description
    }
}

#[derive(Debug)]
pub struct JobHandlerError {
    description: String,
}

impl Display for JobHandlerError {
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        write!(fmt, "{:?}", self)
    }
}

pub trait JobHandlerFactory {
    fn produce(&mut self) -> Box<JobHandler>;
}

pub struct DummyHandlerFactory;

impl JobHandlerFactory for DummyHandlerFactory {
    fn produce(&mut self) -> Box<JobHandler> {
        Box::new(DummyHandler)
    }
}

pub trait JobHandler: Send {
    fn handle(&mut self, job: &Job) -> Result<(), JobHandlerError>;
}

pub struct DummyHandler;

unsafe impl Sync for DummyHandler {}

impl JobHandler for DummyHandler {
    fn handle(&mut self, job: &Job) -> Result<(), JobHandlerError> {
        info!("handling {}", job.class);
        thread::sleep(Duration::from_secs(2));
        Ok(())
    }
}