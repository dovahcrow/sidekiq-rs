use chrono::UTC;


use utils::rust_gethostname;
use errors::*;

pub trait Reporter {
    fn report_alive(&mut self) -> Result<()> {
        let now = UTC::now();

        let content = vec![("info",
                            to_string(&json!({
                                "hostname": rust_gethostname().unwrap_or("unknown".into()),
                                "started_at": self.started_at,
                                "pid": self.pid,
                                "concurrency": self.concurrency,
                                "queues": self.queues.clone(),
                                "labels": [],
                                "identity": self.identity()
                            }))
                                .unwrap()),
                           ("busy", self.worker_info.values().filter(|v| **v).count().to_string()),
                           ("beat",
                            (now.timestamp() as f64 +
                             now.timestamp_subsec_micros() as f64 / 1000000f64)
                                .to_string())];
        let conn = self.redis_pool.get()?;
        try!(Pipeline::new()
            .hset_multiple(self.with_namespace(&self.identity()), &content)
            .expire(self.with_namespace(&self.identity()), 5)
            .sadd(self.with_namespace(&"processes"), self.identity())
            .query::<()>(&*conn));

        Ok(())

    }


    fn report_processed(&mut self, n: usize) -> Result<()> {
        let connection = try!(self.redispool.get());
        let _: () = Pipeline::new().incr(self.with_namespace(&format!("stat:processed:{}",
                                               UTC::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:processed")), n)
            .query(&*connection)?;

        Ok(())
    }


    fn report_failed(&mut self, n: usize) -> Result<()> {
        let connection = try!(self.redispool.get());
        let _: () = Pipeline::new()
            .incr(self.with_namespace(&format!("stat:failed:{}", UTC::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:failed")), n)
            .query(&*connection)?;
        Ok(())
    }
}