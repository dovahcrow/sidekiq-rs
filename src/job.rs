use serde_json::Value as JValue;

#[derive(Serialize, Deserialize, Debug)]
pub struct Job {
    pub class: String,
    pub args: Vec<JValue>,
    pub queue: String,
    // retry: usize,
    pub jid: String,
    pub created_at: f64,
    pub enqueued_at: f64,
}