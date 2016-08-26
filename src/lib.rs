#![feature(plugin, custom_derive)]
#![feature(question_mark)]
#![plugin(serde_macros)]

extern crate serde;
extern crate serde_json;
extern crate rustc_serialize;

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate error_chain;

extern crate threadpool;
extern crate redis;
extern crate r2d2;
extern crate r2d2_redis;

extern crate rand;
extern crate random_choice;

extern crate libc;

#[macro_use]
extern crate json;
extern crate chrono;

#[macro_use]
extern crate chan;


mod server;
mod job_handler;
mod errors;
mod job;
mod utils;
mod worker;


pub use server::SidekiqServer;
pub use job_handler::{DummyHandlerFactory, JobHandlerFactory, JobHandler, JobHandlerError};
pub use errors::*;
