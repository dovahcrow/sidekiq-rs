error_chain!{
    types {}
    links {}
    foreign_links {
         ::redis::RedisError, RedisError;
         ::serde_json::Error, JsonError;
         ::job_handler::JobHandlerError, JobHandlerError;
    }
    errors {
         RubyExceptionError(t: String) {
             description("meet ruby exception")
             display("meet ruby exception")
         }
    }
}