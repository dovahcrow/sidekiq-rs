use std::error::Error as StdError;
error_chain!{
    foreign_links {
         RedisError(::redis::RedisError) ;
         JsonError(::serde_json::Error);
         R2D2TimeoutError(::r2d2::GetTimeout);
         R2D2InitializerError(::r2d2::InitializationError);
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