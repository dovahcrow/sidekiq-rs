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
         RubyExceptionError(t: String) {
             description("meet ruby exception")
             display("meet ruby exception")
         }
    }
}