#![feature(attr_literals)]
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
extern crate sidekiq;
extern crate env_logger;

use sidekiq::{ErrorHandler, PanicHandler, PrinterHandler, RetryMiddleware, TimeElapseMiddleware,
              SidekiqServerBuilder, FutureJob, JobAgent};
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "example", about = "An example of Sidekiq usage.", author = "Young Wu <doomsplayer@gmail.com")]
struct Params {
    #[structopt(short = "r", long = "redis", help = "redis connection string", default_value = "redis://localhost:6379")]
    redis: String,
    #[structopt(short = "n", long = "namespace", help = "the namespace", default_value = "")]
    namespace: String,
    #[structopt(short = "c", long = "concurrency", help = "how many workers do you want to start", default_value = "10")]
    concurrency: usize,
    #[structopt(short = "q", long = "queues", required = true, multiple = true, help = "the queues, in `name:weight` format, e.g. `critical:10`")]
    queues: Vec<String>,
    #[structopt(short = "t", long = "timeout", help = "the timeout when force terminated", default_value = "10")]
    timeout: usize,
}

fn main() {
    env_logger::init().unwrap();
    let params = Params::from_args();

    let queues: Vec<_> = params.queues
        .into_iter()
        .map(|v| {
            let mut sp = v.split(':');
            let name = sp.next().unwrap();
            let weight = sp.next().unwrap().parse().unwrap();
            (name.to_string(), weight)
        })
        .collect();

    let mut builder = SidekiqServerBuilder::new();
    builder.concurrency(params.concurrency)
        .job_handler("Printer", PrinterHandler as fn(JobAgent) -> JobAgent)
        .job_handler("Error", ErrorHandler as fn(JobAgent) -> FutureJob)
        .job_handler("Panic", PanicHandler as fn())
        .middleware(RetryMiddleware)
        .middleware(TimeElapseMiddleware::new());
    for (name, weight) in queues {
        builder.queue(&name, weight);
    }
    let server = builder.build(&params.redis).expect("cannot create sidekiq server");
    server.start()
}