pub use aws_sdk_sqs::Client;
pub use aws_sdk_sqs::model::Message;
pub use aws_sdk_sqs::model::MessageAttributeValue;
pub use aws_sdk_sqs::model::MessageSystemAttributeName;
pub use aws_sdk_sqs::model::QueueAttributeName;
use tracing::Instrument;

use crate::utils::MessageUtils;

const QUEUE_TIMEOUT_SECS: u16 = 30;

pub async fn get_queue_timeout_secs(client: &Client, queue_url: &str) -> u16 {
    match client.get_queue_attributes().queue_url(queue_url).attribute_names(QueueAttributeName::VisibilityTimeout).send().await {
        Err(e) => {
            tracing::warn!("Queue {queue_url}: unable to establish queue timeout, using default {QUEUE_TIMEOUT_SECS} secs: {}", e.to_string());
            QUEUE_TIMEOUT_SECS
        },
        Ok(gqa) => {
            match gqa.attributes() {
                None => {
                        tracing::debug!("Queue {queue_url}: no attrs, using default {QUEUE_TIMEOUT_SECS} secs");
                        QUEUE_TIMEOUT_SECS
                }
                Some(attrs) => match attrs.get(&aws_sdk_sqs::model::QueueAttributeName::VisibilityTimeout) {
                    None => {
                        tracing::debug!("Queue {queue_url}: no timeout attr found, using default {QUEUE_TIMEOUT_SECS} secs");
                        QUEUE_TIMEOUT_SECS
                    },
                    Some(vt) => {
                        tracing::debug!("Queue {queue_url}: parsing queue tiemout {vt}");
                        vt.parse().unwrap_or(QUEUE_TIMEOUT_SECS)
                    }
                }
            }
        }
    }
}

pub fn hold_message_lease(client: &Client, queue_url: &str, m: &Message, timeout_secs: u16) -> Option<tokio::task::JoinHandle<()>> {
    if let Some(rcpt_handle) = m.receipt_handle().map(|s| s.to_owned()) {
        let client = client.clone();
        let queue_url = queue_url.to_owned();
        let msg_id = m.display_id();
        let interval = renewal_interval_for_timeout_secs(timeout_secs);

        let join_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                ticker.tick().await;
                tracing::info!("Message {msg_id}: renewing lease via {rcpt_handle}");
                let cmv = client
                    .change_message_visibility()
                    .queue_url(&queue_url)
                    .receipt_handle(&rcpt_handle)
                    .visibility_timeout(timeout_secs.into())
                    .send()
                    .await;
                if let Err(e) = cmv {
                    tracing::error!("Message {msg_id}: failed to update lease: {}", e.to_string());
                }
            }
        }.in_current_span());
       
        Some(join_handle)
    } else {
        None
    }
}

fn renewal_interval_for_timeout_secs(timeout_secs: u16) -> tokio::time::Duration {
    tokio::time::Duration::from_secs((timeout_secs / 2).into())
}