extern crate structopt;
#[macro_use]
extern crate structopt_derive;
extern crate sidekiq;
extern crate env_logger;

use sidekiq::{error_handler, panic_handler, printer_handler, retry_middleware, SidekiqServer};
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
    #[structopt(short = "q", long = "queues", help = "the queues, in `name:weight` format, e.g. `critical:10`")]
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


    let mut server = SidekiqServer::new(&params.redis, params.concurrency).unwrap();

    server.attach_handler("Printer", printer_handler);
    server.attach_handler("Error", error_handler);
    server.attach_handler("Panic", panic_handler);

    server.attach_middleware(retry_middleware);
    for (name, weight) in queues {
        server.new_queue(&name, weight);
    }

    server.namespace = params.namespace;
    server.force_quite_timeout = params.timeout;
    start(server)
}

fn start(mut server: SidekiqServer) {
    server.start();
}