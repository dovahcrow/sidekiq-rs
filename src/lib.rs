#![feature(plugin, custom_derive)]
#![feature(question_mark)]
#![plugin(serde_macros, docopt_macros)]

#[macro_use]
extern crate ruru;
extern crate serde;
extern crate serde_json;
extern crate rustc_serialize;

#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate env_logger;
extern crate threadpool;
extern crate random_choice;
extern crate redis;
extern crate libc;
extern crate rand;
#[macro_use]
extern crate json;
extern crate chrono;

extern crate r2d2;
extern crate r2d2_redis;
#[macro_use]
extern crate chan;

pub use server::SidekiqServer;
pub use job_handler::{DummyHandlerFactory, JobHandlerFactory, JobHandler, JobHandlerError};
pub use errors::*;

mod server;
mod job_handler;
mod errors;
mod job;
mod utils;
mod worker;
