use futures::Future;
use futures::future::{ok, err};

use job_agent::JobAgent;
use errors::ErrorKind;
use FutureJob;

pub trait JobHandler: Send {
    fn perform(&mut self, continuation: FutureJob) -> FutureJob;
    fn cloned(&mut self) -> Box<JobHandler>;
}

#[allow(non_snake_case)]
pub fn PrinterHandler(continuation: JobAgent) -> JobAgent {
    info!("handling {:?}", *continuation);
    continuation
}

#[allow(non_snake_case)]
pub fn ErrorHandler(job: JobAgent) -> FutureJob {
    err((job, ErrorKind::ZeroConcurrency.into())).boxed()
}

#[allow(non_snake_case)]
pub fn PanicHandler() {
    panic!("yeah, I do it deliberately")
}

macro_rules! fn_impl_body {
    (fn(&'a mut JobAgent), $continuation: expr, $ego: expr) => (
        $continuation.and_then(move |job| {
            $ego(&mut job);
            ok(job)
        }).boxed()
    );
    (fn(&'a JobAgent), $continuation: expr, $ego: expr) => (
        $continuation.and_then(move |job| {
            $ego(&job);
            ok(job)
        }).boxed()
    );
    (fn(JobAgent) -> JobAgent, $continuation: expr, $ego: expr) => (
        $continuation.and_then(move |job| ok($ego(job))).boxed()
    );
    (fn(JobAgent) -> FutureJob, $continuation: expr, $ego: expr) => (
        $continuation.and_then(move |job| {
            $ego(job)
        }).boxed()
    );
    (fn(FutureJob) -> FutureJob, $continuation: expr, $ego: expr) => (
        $ego($continuation)
    );
    (fn(), $continuation: expr, $ego: expr) => ({
        $ego();
        $continuation
    })
}

macro_rules! fn_impl {
    ('a @ $($t: tt)*) => (
        impl<'a> JobHandler for $($t)* {
            fn perform(&mut self, continuation: FutureJob) -> FutureJob {
                let ego = self.clone();
                fn_impl_body!($($t)*, continuation, ego)
            }
            fn cloned(&mut self) -> Box<JobHandler> {
                Box::new(*self)
            }
        }
    );
    ($($t: tt)*) => (
        impl JobHandler for $($t)* {
            fn perform(&mut self, continuation: FutureJob) -> FutureJob {
                let ego = self.clone();
                fn_impl_body!($($t)*, continuation, ego)
            }
            fn cloned(&mut self) -> Box<JobHandler> {
                Box::new(*self)
            }
        }
    );
}

// impl<'a> JobHandler for fn(&'a JobAgent) {
//     fn perform(&mut self, continuation: FutureJob) -> FutureJob {
//         let ego: fn(&'a JobAgent) = self.clone();
//         continuation.join(ok(ego))
//             .and_then(move |(job, ego)| {
//                 ego(&job);
//                 ok(job)
//             })
//             .boxed()
//     }
//     fn cloned(&mut self) -> Box<JobHandler> {
//         Box::new(*self)
//     }
// }

//fn_impl! { 'a @ fn(&'a JobAgent) }
//fn_impl! { 'a @ fn(&'a mut JobAgent) }
fn_impl! { fn(JobAgent) -> JobAgent }
fn_impl! { fn(JobAgent) -> FutureJob }
fn_impl! { fn(FutureJob) -> FutureJob }
fn_impl! { fn() }