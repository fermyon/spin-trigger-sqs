use std::{sync::Arc, collections::HashMap};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use spin_trigger::{cli::NoArgs, TriggerAppEngine, TriggerExecutor};
use spin_core::InstancePre;

mod aws;
mod utils;

use tracing::{instrument, Instrument, Level};
use utils::MessageUtils;

wasmtime::component::bindgen!({
    path: "sqs.wit",
    async: true
});

use fermyon::spin_sqs::sqs_types as sqs;

pub(crate) type RuntimeData = ();

pub struct SqsTrigger {
    engine: TriggerAppEngine<Self>,
    queue_components: Vec<Component>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqsTriggerConfig {
    pub component: String,
    pub queue_url: String,
    pub max_messages: Option<u16>,
    pub idle_wait_seconds: Option<u64>,
    pub system_attributes: Option<Vec<String>>,
    pub message_attributes: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
struct Component {
    pub id: String,
    pub queue_url: String,
    pub max_messages: u16,
    pub idle_wait: tokio::time::Duration,
    pub system_attributes: Vec<aws::QueueAttributeName>,
    pub message_attributes: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TriggerMetadata {
    r#type: String,
}

// This is a placeholder - we don't yet detect any situations that would require
// graceful or ungraceful exit.  It will likely require rework when we do.  It
// is here so that we have a skeleton for returning errors that doesn't expose
// us to thoughtlessly "?"-ing away an Err case and creating a situation where a
// transient failure could end the trigger.
#[allow(dead_code)]
#[derive(Debug)]
enum TerminationReason {
    ExitRequested,
    Other(String),
}

#[async_trait]
impl TriggerExecutor for SqsTrigger {
    const TRIGGER_TYPE: &'static str = "sqs";
    type RuntimeData = RuntimeData;
    type TriggerConfig = SqsTriggerConfig;
    type RunConfig = NoArgs;
    type InstancePre = InstancePre<RuntimeData>;

    async fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let queue_components = engine
            .trigger_configs()
            .map(|(_, config)| Component {
                id: config.component.clone(),
                queue_url: config.queue_url.clone(),
                max_messages: config.max_messages.unwrap_or(10).into(),
                idle_wait: tokio::time::Duration::from_secs(config.idle_wait_seconds.unwrap_or(2)),
                system_attributes: config.system_attributes.clone().unwrap_or_default().iter().map(|s| s.as_str().into()).collect(),
                message_attributes: config.message_attributes.clone().unwrap_or_default(),
            })
            .collect();

        Ok(Self {
            engine,
            queue_components,
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            std::process::exit(0);
        });

        let config = aws_config::load_from_env().await;

        let client = aws::Client::new(&config);
        let engine = Arc::new(self.engine);

        let loops = self.queue_components.iter().map(|component| {
            Self::start_receive_loop(engine.clone(), &client, component)
        });

        let (tr, _, rest) = futures::future::select_all(loops).await;
        drop(rest);

        match tr {
            Ok(TerminationReason::ExitRequested) => {
                tracing::trace!("Exiting");
                Ok(())
            },
            _ => {
                tracing::trace!("Fatal: {:?}", tr);
                Err(anyhow::anyhow!("{tr:?}"))
            }
        }
    }
}

impl SqsTrigger {
    fn start_receive_loop(engine: Arc<TriggerAppEngine<Self>>, client: &aws::Client, component: &Component) -> tokio::task::JoinHandle<TerminationReason> {
        let future = Self::receive(engine, client.clone(), component.clone());
        tokio::task::spawn(future.in_current_span())
    }

    // This doesn't return a Result because we don't want a thoughtless `?` to exit the loop
    // and terminate the entire trigger.  Termination should be a conscious decision when
    // we are sure there is no point continuing.
    async fn receive(engine: Arc<TriggerAppEngine<Self>>, client: aws::Client, component: Component) -> TerminationReason {
        let queue_timeout_secs = aws::get_queue_timeout_secs(&client, &component.queue_url).await;

        loop {
            tracing::trace!("Queue {}: attempting to receive up to {}", component.queue_url, component.max_messages);

            let rmo = match client
                .receive_message()
                .queue_url(&component.queue_url)
                .max_number_of_messages(component.max_messages.into())
                .set_attribute_names(Some(component.system_attributes.clone()))
                .set_message_attribute_names(Some(component.message_attributes.clone()))
                .send()
                .await
            {
                Ok(rmo) => rmo,
                Err(e) => {
                    tracing::error!("Queue {}: error receiving messages: {:?}", component.queue_url, e);
                    tokio::time::sleep(component.idle_wait).await;
                    continue;
                }
            };

            if let Some(msgs) = rmo.messages() {
                let msgs = msgs.to_vec();
                tracing::info!("Queue {}: received {} message(s)", component.queue_url, msgs.len());
                for msg in msgs {
                    // Spin off the execution so it doesn't block the queue
                    let processor = SqsMessageProcessor::new(&engine, &client, &component, queue_timeout_secs);
                    tokio::spawn(async move {
                        processor.process_message(msg).await
                    }.in_current_span());
                }
            } else {
                tracing::trace!("Queue {}: no messages received", component.queue_url);
                tokio::time::sleep(component.idle_wait).await;
            }
        }
    }
}

struct SqsMessageProcessor {
    engine: Arc<TriggerAppEngine<SqsTrigger>>,
    client: aws::Client,
    component: Component,
    queue_timeout_secs: u16,
}

impl SqsMessageProcessor {
    fn new(
        engine: &Arc<TriggerAppEngine<SqsTrigger>>,
        client: &aws::Client,
        component: &Component,
        queue_timeout_secs: u16
    ) -> Self {
        Self {
            engine: engine.clone(),
            client: client.clone(),
            component: component.clone(),
            queue_timeout_secs
        }
    }
    #[instrument(name = "spin_trigger_sqs.process_message", skip_all, fields(otel.name = format!("{} receive", queue_name_from_url(&self.component.queue_url)), messaging.message.id = msg.display_id()))]
    async fn process_message(&self, msg: aws::Message) {
        let msg_id = msg.display_id();
        tracing::trace!("Message {msg_id}: spawned processing task");

        // The attr lists have to be returned to this level so that they live long enough
        let attrs = to_wit_message_attrs(&msg);
        let message = sqs::Message {
            id: msg.message_id().map(|s| s.to_owned()),
            message_attributes: attrs,
            body: msg.body().map(|s| s.to_owned()),
        };

        let renew_lease = aws::hold_message_lease(&self.client, self.queue_url(), &msg, self.queue_timeout_secs);

        let action = self.execute_wasm(message).await;

        if let Some(renewer) = renew_lease {
            renewer.abort();
        }

        match action {
            Ok(sqs::MessageAction::Delete) => {
                tracing::trace!("Message {msg_id} processed successfully: action is Delete");
                if let Some(receipt_handle) = msg.receipt_handle() {
                    tracing::trace!("Message {msg_id}: attempting to delete via {receipt_handle}");
                    match self.client.delete_message().queue_url(self.queue_url()).receipt_handle(receipt_handle).send().await {
                        Ok(_) => tracing::trace!("Message {msg_id} deleted"),
                        Err(e) => tracing::error!("Message {msg_id}: error deleting via {receipt_handle}: {e:?}"),
                    }
                }
            }
            Ok(sqs::MessageAction::Leave) => {
                tracing::trace!("Message {msg_id} processed successfully: action is Leave");
                // TODO: change message visibility to 0?
            }
            Err(e) => {
                tracing::error!("Message {msg_id} processing error: {}", e.to_string());
                // TODO: change message visibility to 0 I guess?
            }
        }
    }
    #[instrument(name = "spin_trigger_sqs.execute_wasm", skip_all, err(level = Level::INFO), fields(otel.name = format!("execute_wasm_component {}", self.component.id)))]
    async fn execute_wasm(&self, message: sqs::Message) -> Result<sqs::MessageAction> {
        let msg_id = message.display_id();
        let component_id = &self.component.id;
        tracing::trace!("Message {msg_id}: executing component {component_id}");
        let (instance, mut store) = self.engine.prepare_instance(component_id).await?;

        let instance = SpinSqs::new(&mut store, &instance)?;

        match instance.call_handle_queue_message(&mut store, &message).await {
            Ok(Ok(action)) => {
                tracing::trace!("Message {msg_id}: component {component_id} completed okay");
                Ok(action)
            },
            Ok(Err(e)) => {
                tracing::warn!("Message {msg_id}: component {component_id} returned error {:?}", e);
                Err(anyhow::anyhow!("Component {component_id} returned error processing message {msg_id}"))  // TODO: more details when WIT provides them
            },
            Err(e) => {
                tracing::error!("Message {msg_id}: engine error running component {component_id}: {:?}", e);
                Err(anyhow::anyhow!("Error executing component {component_id} while processing message {msg_id}"))
            },
        }
    }

    fn queue_url(&self) -> &str {
        &self.component.queue_url
    }
}

fn to_wit_message_attrs(m: &aws::Message) -> Vec<sqs::MessageAttribute> {
    let msg_id = m.display_id();

    let sysattrs = m.attributes()
        .map(system_attributes_to_wit)
        .unwrap_or_default();
    let userattrs = m.message_attributes()
        .map(|a| user_attributes_to_wit(a, &msg_id))
        .unwrap_or_default();

    vec![sysattrs, userattrs].concat()
}

fn system_attributes_to_wit(src: &HashMap<aws::MessageSystemAttributeName, String>) -> Vec<sqs::MessageAttribute> {
    src
    .iter()
    .map(|(k, v)| sqs::MessageAttribute {
        name: k.as_str().to_string(),
        value: sqs::MessageAttributeValue::Str(v.to_string()),
        data_type: None
    })
    .collect::<Vec<_>>()
}

fn user_attributes_to_wit(src: &HashMap<String, aws::MessageAttributeValue>, msg_id: &str) -> Vec<sqs::MessageAttribute> {
    src
    .iter()
    .filter_map(|(k, v)| {
        match wit_value(v) {
            Ok(wv) => Some(sqs::MessageAttribute {
                name: k.to_string(),
                value: wv,
                data_type: None
            }),
            Err(e) => {
                tracing::error!("Message {msg_id}: can't convert attribute {} to string or blob, skipped: {e:?}", k.as_str());  // TODO: this should probably fail the message
                None
            },
        }
    })
    .collect::<Vec<_>>()
}

fn wit_value(v: &aws::MessageAttributeValue) -> Result<sqs::MessageAttributeValue> {
    if let Some(s) = v.string_value() {
        Ok(sqs::MessageAttributeValue::Str(s.to_string()))
    } else if let Some(b) = v.binary_value() {
        Ok(sqs::MessageAttributeValue::Binary(b.as_ref().to_vec()))
    } else {
        Err(anyhow::anyhow!("Don't know what to do with message attribute value {:?} (data type {:?})", v, v.data_type()))
    }
}

fn queue_name_from_url(url: &str) -> &str {
    url.rsplit('/').next().unwrap_or(url)
}