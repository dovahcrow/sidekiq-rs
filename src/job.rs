use std::collections::BTreeMap;
use std::iter::FromIterator;

use serde_json::{Value as JValue, Map as JMap};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::Error;
use serde::ser::SerializeMap;

use chrono::{DateTime, UTC, NaiveDateTime};

#[derive(Debug, Clone)]
pub enum BoolOrUSize {
    Bool(bool),
    USize(usize),
}

#[derive(Debug, Clone)]
pub struct Job {
    pub class: String,
    pub jid: String,
    pub args: Vec<JValue>,
    pub created_at: Option<DateTime<UTC>>,
    pub enqueued_at: DateTime<UTC>,
    pub queue: String,
    pub retry: BoolOrUSize,
    pub at: Option<DateTime<UTC>>, // when scheduled
    pub namespace: String,
    pub retry_info: Option<RetryInfo>,
    pub extra: BTreeMap<String, JValue>,
}

impl Job {
    pub fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }

    pub fn queue_name(&self) -> String {
        self.with_namespace(&("queue:".to_string() + &self.queue))
    }
}

impl Deserialize for Job {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let j = <JValue as Deserialize>::deserialize(deserializer)?;
        if let JValue::Object(mut obj) = j {
            let class = JMapExt::<D>::remove_string(&mut obj, "class")?;
            let args = JMapExt::<D>::remove_vec(&mut obj, "args")?;
            let queue = JMapExt::<D>::remove_string(&mut obj, "queue")?;
            let jid = JMapExt::<D>::remove_string(&mut obj, "jid")?;
            let retry = obj.remove("retry")
                .and_then(|r| if r.is_u64() {
                    Some(BoolOrUSize::USize(r.as_u64().unwrap() as usize))
                } else if r.is_boolean() {
                    Some(BoolOrUSize::Bool(r.as_bool().unwrap()))
                } else {
                    None
                })
                .ok_or(D::Error::custom("no member 'retry'"))?;
            let created_at = JMapExt::<D>::remove_datetime(&mut obj, "created_at").ok();
            let enqueued_at = JMapExt::<D>::remove_datetime(&mut obj, "enqueued_at")?;
            let at = JMapExt::<D>::remove_datetime(&mut obj, "at").ok();

            let retry_info = hado! {
                    retry_count <- JMapExt::<D>::remove_usize(&mut obj, "retry_count").ok();
                    error_message <- JMapExt::<D>::remove_string(&mut obj, "error_message").ok();
                    error_class <- JMapExt::<D>::remove_string(&mut obj, "error_class").ok();
                    error_backtrace <- JMapExt::<D>::remove_svec(&mut obj, "error_backtrace").ok();
                    failed_at <- JMapExt::<D>::remove_datetime(&mut obj, "failed_at").ok();
                    retried_at <- JMapExt::<D>::remove_datetime(&mut obj, "retried_at").ok();

                    Some(RetryInfo {
                        retry_count: retry_count,
                        error_message: error_message.clone(),
                        error_class: error_class.clone(),
                        error_backtrace: error_backtrace.clone(),
                        failed_at: failed_at,
                        retried_at: retried_at,
                    })
                };

            Ok(Job {
                class: class,
                args: args,
                queue: queue,
                jid: jid,
                retry: retry,
                created_at: created_at,
                enqueued_at: enqueued_at,
                at: at,
                extra: BTreeMap::from_iter(obj),
                retry_info: retry_info,
                namespace: "".into(), // it will be set later on
            })
        } else {
            Err(D::Error::custom("not an object"))
        }
    }
}

impl Serialize for Job {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut map_serializer = serializer.serialize_map(None)?;

        map_serializer.serialize_entry("class", &self.class)?;
        map_serializer.serialize_entry("args", &self.args)?;
        map_serializer.serialize_entry("queue", &self.queue)?;
        map_serializer.serialize_entry("jid", &self.jid)?;

        self.created_at
            .map(|created_at| {
                let timestamp = created_at.timestamp() as f64 +
                                created_at.timestamp_subsec_nanos() as f64 / 1e9;
                map_serializer.serialize_entry("created_at", &timestamp)
            })
            .unwrap_or(Ok(()))?;

        let enqueued_at = self.enqueued_at.timestamp() as f64 +
                          self.enqueued_at.timestamp_subsec_nanos() as f64 / 1e9;
        map_serializer.serialize_entry("enqueued_at", &enqueued_at)?;

        self.at
            .map(|at| {
                let timestamp = at.timestamp() as f64 + at.timestamp_subsec_nanos() as f64 / 1e9;
                map_serializer.serialize_entry("at", &timestamp)
            })
            .unwrap_or(Ok(()))?;

        match self.retry {
            BoolOrUSize::Bool(x) => {
                map_serializer.serialize_entry("retry", &x)?;
            }
            BoolOrUSize::USize(x) => {
                map_serializer.serialize_entry("retry", &x)?;
            }
        };

        if let Some(ref retry_info) = self.retry_info {
            map_serializer.serialize_entry("error_backtrace", &retry_info.error_backtrace)?;
            map_serializer.serialize_entry("error_class", &retry_info.error_class)?;
            map_serializer.serialize_entry("error_message", &retry_info.error_message)?;
            let failed_at = retry_info.failed_at.timestamp() as f64 +
                            retry_info.failed_at.timestamp_subsec_nanos() as f64 / 1e9;
            map_serializer.serialize_entry("failed_at", &failed_at)?;

            let retried_at = retry_info.retried_at.timestamp() as f64 +
                             retry_info.retried_at.timestamp_subsec_nanos() as f64 / 1e9;
            map_serializer.serialize_entry("retried_at", &retried_at)?;

            map_serializer.serialize_entry("retry_count", &retry_info.retry_count)?;
        }
        for (k, v) in &self.extra {
            map_serializer.serialize_entry(k, v)?;
        }

        map_serializer.end()

    }
}

#[derive(Clone, Debug)]
pub struct RetryInfo {
    pub retry_count: usize,
    pub error_message: String,
    pub error_class: String,
    pub error_backtrace: Vec<String>,
    pub failed_at: DateTime<UTC>,
    pub retried_at: DateTime<UTC>,
}

trait JMapExt<D>
    where D: Deserializer
{
    fn remove_datetime(&mut self, key: &str) -> Result<DateTime<UTC>, D::Error>;
    fn remove_string(&mut self, key: &str) -> Result<String, D::Error>;
    fn remove_vec(&mut self, key: &str) -> Result<Vec<JValue>, D::Error>;
    fn remove_svec(&mut self, key: &str) -> Result<Vec<String>, D::Error>;
    fn remove_usize(&mut self, key: &str) -> Result<usize, D::Error>;
}

impl<D> JMapExt<D> for JMap<String, JValue>
    where D: Deserializer
{
    fn remove_datetime(&mut self, key: &str) -> Result<DateTime<UTC>, D::Error> {
        self.remove(key)
            .and_then(|v| v.as_f64())
            .map(|f| NaiveDateTime::from_timestamp(f as i64, ((f - f.floor()) * 1e9) as u32))
            .map(|t| DateTime::from_utc(t, UTC))
            .ok_or(D::Error::custom(format!("no member '{}'", key)))
    }

    fn remove_vec(&mut self, key: &str) -> Result<Vec<JValue>, D::Error> {
        let value = self.remove(key);
        match value {
            Some(JValue::Array(v)) => Ok(v),
            Some(_) => Err(D::Error::custom(format!("'{}' not a array", key))),
            None => Err(D::Error::custom(format!("no member '{}'", key))),
        }
    }

    fn remove_svec(&mut self, key: &str) -> Result<Vec<String>, D::Error> {
        let value = self.remove(key);
        match value {
            Some(JValue::Array(v)) => {
                v.into_iter()
                    .map(|e| match e {
                        JValue::String(s) => Ok(s),
                        _ => Err(D::Error::custom(format!("'{}' contains non string", key))),
                    })
                    .collect()
            }
            Some(_) => Err(D::Error::custom(format!("'{}' not a array", key))),
            None => Err(D::Error::custom(format!("no member '{}'", key))),
        }
    }

    fn remove_string(&mut self, key: &str) -> Result<String, D::Error> {
        self.remove(key)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .ok_or(D::Error::custom(format!("no member '{}'", key)))
    }

    fn remove_usize(&mut self, key: &str) -> Result<usize, D::Error> {
        match self.remove(key) {
            Some(JValue::Number(number)) => Ok(number.as_f64().unwrap_or(0f64) as usize),
            Some(_) => Err(D::Error::custom(format!("'{}' not a usize", key))),
            None => Err(D::Error::custom(format!("no member '{}'", key))),
        }
    }
}