#![feature(trace_macros,log_syntax)]
#![recursion_limit="1024"]
extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate redis;
extern crate r2d2;
extern crate r2d2_redis;
extern crate rand;
extern crate random_choice;
extern crate libc;
extern crate chrono;
#[macro_use]
extern crate hado;
#[macro_use]
extern crate chan;
extern crate chan_signal;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate derive_more;

mod server;
mod job_handler;
pub mod errors;
mod job;
mod utils;
mod middleware;
mod job_agent;

use r2d2::Pool;
use r2d2_redis::RedisConnectionManager;
use futures::BoxFuture;


pub use server::{SidekiqServerBuilder, SidekiqServer};
pub use job_handler::{JobHandler, PrinterHandler, ErrorHandler, PanicHandler};
pub use middleware::{MiddleWare, PeekMiddleware, TimeElapseMiddleware, RetryMiddleware};
pub use job::{Job, RetryInfo};
pub use job_agent::JobAgent;

pub type RedisPool = Pool<RedisConnectionManager>;
pub type FutureJob = BoxFuture<job_agent::JobAgent, (job_agent::JobAgent, errors::Error)>;