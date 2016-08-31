use std::error::Error as StdError;
error_chain!{
    types {}
    links {}
    foreign_links {
         ::redis::RedisError, RedisError;
         ::serde_json::Error, JsonError;
         ::r2d2::GetTimeout, R2D2Error;
    }
    errors {
         WorkerError(t: String) {
             description("Worker error")
             display("Worker Error '{}'", t)
         }
         JobHandlerError(e: Box<StdError+Send>) {
             description("Job handler error")
             display("Job handler error '{}'",e)
         }
         MiddleWareError(e: Box<StdError+Send>) {
             description("Middleware error")
             display("Middleware error '{}'", e)
         }
    }
}