use ::job::Job;
use errors::{ErrorKind, Result};

pub type JobHandlerResult = Result<()>;

pub trait JobHandler: Send {
    fn handle(&mut self, job: &Job) -> JobHandlerResult;
    fn cloned(&mut self) -> Box<JobHandler>;
}

#[derive(Clone)]
pub struct PrinterHandler;

impl JobHandler for PrinterHandler {
    fn handle(&mut self, job: &Job) -> JobHandlerResult {
        info!("handling {:?}", job);
        Ok(())
    }
    fn cloned(&mut self) -> Box<JobHandler> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
pub struct ErrorHandler;


impl JobHandler for ErrorHandler {
    fn handle(&mut self, _: &Job) -> JobHandlerResult {
        Err(ErrorKind::JobHandlerError(Box::new("a".parse::<i8>().unwrap_err())).into())
    }
    fn cloned(&mut self) -> Box<JobHandler> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
pub struct PanicHandler;

impl JobHandler for PanicHandler {
    fn handle(&mut self, _: &Job) -> JobHandlerResult {
        panic!("yeah, I do it deliberately")
    }
    fn cloned(&mut self) -> Box<JobHandler> {
        Box::new(self.clone())
    }
}