error_chain!{
    types {}
    links {}
    foreign_links {
         ::redis::RedisError, RedisError;
         ::serde_json::Error, JsonError;
         ::r2d2::GetTimeout, R2D2Error;
         ::job_handler::JobHandlerError, JobHandlerError; 
    }
    errors {
         WorkerError(t: String) {
             description("Worker Error")
             display("Worker Error '{}'", t)
         }
    }
}