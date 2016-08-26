run:
	RUST_BACKTRACE=1 RUST_LOG=sidekiqrs=info cargo run --example main --release -- -r redis://localhost:6379 -n annie -c 10 -q analytics:10

debug:
	RUST_BACKTRACE=1 RUST_LOG=sidekiqrs=debug cargo run --example main -- -r redis://localhost:6379 -n annie -c 10 -q analytics:10

push:
	ruby ruby/push.rb