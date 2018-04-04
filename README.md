Sidekiq-rs [![Build Status](https://travis-ci.org/doomsplayer/sidekiq-rs.svg?branch=master)](https://travis-ci.org/doomsplayer/sidekiq-rs) [![Crates.io](https://img.shields.io/crates/v/sidekiq-rs.svg?maxAge=86400)](https://crates.io/crates/sidekiq-rs) [![Crates.io](https://img.shields.io/crates/dv/sidekiq-rs.svg?maxAge=86400)](https://crates.io/crates/sidekiq-rs) [![Crates.io](https://img.shields.io/crates/l/sidekiq-rs.svg?maxAge=86400)](https://crates.io/crates/sidekiq-rs)
====

Sidekiq is a simple and efficient background processing library for Ruby. This repository provieds a sidekiq compatible server in rust, which behaves totally the same as vanilla in terms of  sidekiq's dashboard.

The basic idea is that, since ruby is slow, we write job handlers in rust with native code. So we write job definitions and enqueue jobs in ruby, while implementations and done jobs are written in rust.

## Snapshots:

![dashboard](screenshot/dashboard.png)

![tui](screenshot/tui.png)

## Usage:

1. Implementing your own `JobHandler`
2. Instantiate a sidekiq-rs server and insert your own job handlers
3. Set up dummy jobs with same name as your job handlers in ruby
4. Run sidekiq-rs server and submit jobs from ruby

You can see `examples/main.rs` for building a sidekiq-rs server, and see codes in `ruby/` to get an idea on writing dummy tasks in ruby.

## Advanced usage:

You can definitely embed a ruby VM in your job handlers, running ruby codes when the job is not a native job,
so that it soon becomes a complete sidekiq server with additional native code support.

## Terminate the sidekiq-rs:

sidekiq-rs currently recognizes 2 types of UNIX signals:

* SIGINT
For forcing the server to exit. The server will terminate all workers and exit in exactly certain time. The default time is 10 seconds.

* SIGUSR1
For gracefully exiting the server. The server will inform workers and wait them to quit.

Server will not accept anymore jobs if receives either of the above signals.

## TODO:

- [x] Sidekiq dashboard capability.
- [x] Exit signal handling.
- [x] Support arbitrary fields in job object.
- [x] Middleware support.
- [x] Job retry support via middleware.
- [ ] Documentation.
- [ ] Unique job support via middleware.
- [ ] Ruby code handler
- [ ] Regex handler matching.
