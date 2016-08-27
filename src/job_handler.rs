use ::job::Job;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::thread;
use std::time::Duration;
use std::marker::Sync;


#[derive(Debug)]
pub struct JobHandlerError(Box<Error + Send>);

impl Error for JobHandlerError {
    fn description(&self) -> &str {
        &self.0.description()
    }
}

impl Display for JobHandlerError {
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        write!(fmt, "{:?}", self)
    }
}



pub trait JobHandlerFactory {
    fn produce(&mut self) -> Box<JobHandler>;
}

pub trait JobHandler: Send {
    fn handle(&mut self, job: &Job) -> Result<(), JobHandlerError>;
}


pub struct PrinterHandlerFactory;

impl JobHandlerFactory for PrinterHandlerFactory {
    fn produce(&mut self) -> Box<JobHandler> {
        Box::new(PrinterHandler)
    }
}

pub struct PrinterHandler;

unsafe impl Sync for PrinterHandler {}

impl JobHandler for PrinterHandler {
    fn handle(&mut self, job: &Job) -> Result<(), JobHandlerError> {
        info!("handling {:?}", job);
        thread::sleep(Duration::from_secs(2));
        Ok(())
    }
}


pub struct ErrorHandlerFactory;

impl JobHandlerFactory for ErrorHandlerFactory {
    fn produce(&mut self) -> Box<JobHandler> {
        Box::new(ErrorHandler)
    }
}

pub struct ErrorHandler;

unsafe impl Sync for ErrorHandler {}

impl JobHandler for ErrorHandler {
    fn handle(&mut self, _: &Job) -> Result<(), JobHandlerError> {
        Err(JobHandlerError(Box::new("a".parse::<i8>().unwrap_err())))
    }
}


pub struct PanicHandlerFactory;

impl JobHandlerFactory for PanicHandlerFactory {
    fn produce(&mut self) -> Box<JobHandler> {
        Box::new(PanicHandler)
    }
}

pub struct PanicHandler;

unsafe impl Sync for PanicHandler {}

impl JobHandler for PanicHandler {
    fn handle(&mut self, _: &Job) -> Result<(), JobHandlerError> {
        panic!("yeah, I do it deliberately")
    }
}