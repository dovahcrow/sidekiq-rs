#![plugin(docopt_macros)]
#![feature(plugin)]
extern crate docopt;
extern crate sidekiqrs;
extern crate rustc_serialize;
extern crate env_logger;
use sidekiqrs::*;

docopt!(Args,
        r#"Sidekiq
Usage: sidekiq -r <redis> -n <namespace> -c <concurrency> (-q <queue>...)

Options:
    -r <redis>, --redis <redis>  redis connection string [default: redis://localhost:6379].
    -n <namespace>, --namespace <namespace>  the namespace.
    -c <concurrency>, --concurrency <concurrency>  how many workers do you want to start.
    (-q <queue>...), (--queues <queue>...)  the queues, in `name:weight` format, e.g. `critial:10`.
"#);


fn main() {
    env_logger::init().unwrap();
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());

    let redis = args.flag_redis;

    let namespace = args.flag_namespace;

    let concurrency = args.flag_concurrency.parse().unwrap();

    let queues: Vec<_> = args.arg_queue
        .into_iter()
        .map(|v| {
            let mut sp = v.split(':');
            let name = sp.next().unwrap();
            let weight = sp.next().unwrap().parse().unwrap();
            (name.to_string(), weight)
        })
        .collect();

    let mut handler = PrinterHandlerFactory;
    let mut handler2 = ErrorHandlerFactory;

    let mut server = SidekiqServer::new(&redis, concurrency);

    server.attach_handler_factory("Dummy", &mut handler);
    server.attach_handler_factory("Error", &mut handler2);

    for (name, weight) in queues {
        server.new_queue(&name, weight);
    }

    if namespace == "" {
        server.namespace = None
    } else {
        server.namespace = Some(namespace)
    }
    server.start();

}