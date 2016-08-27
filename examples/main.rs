extern crate docopt;
extern crate sidekiq;
extern crate rustc_serialize;
extern crate env_logger;
use sidekiq::*;
use docopt::Docopt;


const USAGE: &'static str =       r#"Sidekiq
Usage: sidekiq [-r <redis>] [-n <namespace>] [-c <concurrency>] (-q <queue>...) [-t <timeout>]

Options:
    -r <redis>, --redis <redis>  redis connection string [default: redis://localhost:6379].
    -n <namespace>, --namespace <namespace>  the namespace.
    -c <concurrency>, --concurrency <concurrency>  how many workers do you want to start [default: 10].
    (-q <queue>...), (--queues <queue>...)  the queues, in `name:weight` format, e.g. `critial:10`.
    -t <timeout>, --force-quite-timeout <timeout>  the timeout when force terminated [default: 10].
"#;

#[derive(Debug, RustcDecodable)]
struct Args {
    flag_redis: String,
    flag_namespace: String,
    flag_concurrency: usize,
    arg_queue: Vec<String>,
    flag_t: usize,
}

fn main() {
    env_logger::init().unwrap();
    let args: Args = Docopt::new(USAGE).and_then(|d| d.decode()).unwrap_or_else(|e| e.exit());

    let queues: Vec<_> = args.arg_queue
        .into_iter()
        .map(|v| {
            let mut sp = v.split(':');
            let name = sp.next().unwrap();
            let weight = sp.next().unwrap().parse().unwrap();
            (name.to_string(), weight)
        })
        .collect();

    let mut handler_printer = PrinterHandlerFactory;
    let mut handler_error = ErrorHandlerFactory;
    let mut handler_panic = PanicHandlerFactory;

    let mut server = SidekiqServer::new(&args.flag_redis, args.flag_concurrency);

    server.attach_handler_factory("Printer", &mut handler_printer);
    server.attach_handler_factory("Error", &mut handler_error);
    server.attach_handler_factory("Panic", &mut handler_panic);

    for (name, weight) in queues {
        server.new_queue(&name, weight);
    }

    server.namespace = args.flag_namespace;
    server.force_quite_timeout = args.flag_t;
    server.start();

}