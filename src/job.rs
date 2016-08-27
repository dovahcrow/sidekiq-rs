use serde_json::Value as JValue;
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize, Serializer, Deserializer, Error};

#[derive(Debug)]
pub enum BoolOrUSize {
    Bool(bool),
    USize(usize),
}

#[derive(Debug)]
pub struct Job {
    pub class: String,
    pub args: Vec<JValue>,
    pub queue: String,
    pub retry: BoolOrUSize,
    pub jid: String,
    pub created_at: f64,
    pub enqueued_at: f64,
    pub extra: BTreeMap<String, JValue>,
}


impl Deserialize for Job {
    fn deserialize<D>(deserializer: &mut D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let j = JValue::deserialize(deserializer)?;
        if let Some(obj) = j.as_object() {
            let mut obj = obj.clone();
            Ok(Job {
                class: obj.get("class")
                    .and_then(|v| v.as_str())
                    .ok_or(D::Error::custom("no member 'class'"))?
                    .into(),
                args: obj.get("args")
                    .and_then(|v| v.as_array())
                    .ok_or(D::Error::custom("no member 'args'"))?
                    .clone(),
                queue: obj.get("queue")
                    .and_then(|v| v.as_str())
                    .ok_or(D::Error::custom("no member 'queue'"))?
                    .into(),
                jid: obj.get("jid")
                    .and_then(|v| v.as_str())
                    .ok_or(D::Error::custom("no member 'jid'"))?
                    .into(),
                retry: obj.get("retry")
                    .and_then(|r| {
                        if r.is_u64() {
                            Some(BoolOrUSize::USize(r.as_u64().unwrap() as usize))
                        } else if r.is_boolean() {
                            Some(BoolOrUSize::Bool(r.as_bool().unwrap()))
                        } else {
                            None
                        }
                    })
                    .ok_or(D::Error::custom("no member 'retry'"))?,
                created_at: obj.get("created_at")
                    .and_then(|v| v.as_f64())
                    .ok_or(D::Error::custom("no member 'created_at'"))?
                    .into(),
                enqueued_at: obj.get("enqueued_at")
                    .and_then(|v| v.as_f64())
                    .ok_or(D::Error::custom("no member 'enqueued_at'"))?
                    .into(),
                extra: {
                    obj.remove("class");
                    obj.remove("args");
                    obj.remove("queue");
                    obj.remove("jid");
                    obj.remove("retry");
                    obj.remove("created_at");
                    obj.remove("enqueued_at");
                    obj
                },
            })
        } else {
            Err(D::Error::custom("not an object"))
        }
    }
}

impl Serialize for Job {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: Serializer
    {
        let mut state = serializer.serialize_map(Some(7 + self.extra.len()))?;

        serializer.serialize_map_key(&mut state, "class")?;
        serializer.serialize_map_value(&mut state, &self.class)?;

        serializer.serialize_map_key(&mut state, "args")?;
        serializer.serialize_map_value(&mut state, &self.args)?;

        serializer.serialize_map_key(&mut state, "queue")?;
        serializer.serialize_map_value(&mut state, &self.queue)?;

        serializer.serialize_map_key(&mut state, "jid")?;
        serializer.serialize_map_value(&mut state, &self.jid)?;

        serializer.serialize_map_key(&mut state, "created_at")?;
        serializer.serialize_map_value(&mut state, &self.created_at)?;

        serializer.serialize_map_key(&mut state, "enqueued_at")?;
        serializer.serialize_map_value(&mut state, &self.enqueued_at)?;

        match self.retry {
            BoolOrUSize::Bool(x) => {
                serializer.serialize_map_key(&mut state, "retry")?;
                serializer.serialize_map_value(&mut state, x)?;
            }
            BoolOrUSize::USize(x) => {
                serializer.serialize_map_key(&mut state, "retry")?;
                serializer.serialize_map_value(&mut state, x)?;
            }
        };

        for (k, v) in &self.extra {
            serializer.serialize_map_key(&mut state, k)?;
            serializer.serialize_map_value(&mut state, v)?;
        }

        serializer.serialize_map_end(state)

    }
}