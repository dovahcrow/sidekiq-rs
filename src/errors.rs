use std::error::Error as StdError;
error_chain!{
    foreign_links {
        RedisError(::redis::RedisError) ;
        JsonError(::serde_json::Error);
        R2D2TimeoutError(::r2d2::GetTimeout);
        R2D2InitializerError(::r2d2::InitializationError);
    }
    errors {
        ZeroConcurrency {
            description("ZeroConcurrency")
            display("Concurrency of server should not be zero")
        }
        NoJobHandler {
            description("NoJobHandler")
            display("Server should have at least one job handlers")
        }
        ZeroQueue {
            description("ZeroQueue")
            display("Server should have at least one queue to listen on")
        }
        NoRetryInfo {
            description("NoRetryInfo")
            display("You should provide retry info to put the task back to retry")
        }
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
        UnknownJobClass(class: String) {
            description("UnknownJobClass")
            display("No handler can handle job '{}'", class)
        }
    }
}