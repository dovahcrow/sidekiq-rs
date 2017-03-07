use job::Job;
use JobSuccessType;
use ::JobSuccessType::*;
use errors::{ErrorKind, Result};

pub type JobHandlerResult = Result<JobSuccessType>;

pub trait JobHandler: Send {
    fn handle(&mut self, job: &Job) -> JobHandlerResult;
    fn cloned(&mut self) -> Box<JobHandler>;
}

impl<F> JobHandler for F
    where F: FnMut(&Job) -> JobHandlerResult + Copy + Send + 'static
{
    fn handle(&mut self, job: &Job) -> JobHandlerResult {
        self(job)
    }
    fn cloned(&mut self) -> Box<JobHandler> {
        Box::new(*self)
    }
}

pub fn printer_handler(job: &Job) -> JobHandlerResult {
    info!("handling {:?}", job);
    Ok(Success)
}

pub fn error_handler(_: &Job) -> JobHandlerResult {
    Err(ErrorKind::JobHandlerError(Box::new("a".parse::<i8>().unwrap_err())).into())
}

pub fn panic_handler(_: &Job) -> JobHandlerResult {
    panic!("yeah, I do it deliberately")
}