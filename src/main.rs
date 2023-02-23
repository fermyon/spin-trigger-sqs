use std::{sync::Arc};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use is_terminal::IsTerminal;
use serde::{Deserialize, Serialize};
use sqs::Sqs;
use spin_trigger::{cli::{NoArgs, TriggerExecutorCommand}, TriggerAppEngine, TriggerExecutor};

const QUEUE_TIMEOUT_SECS: u16 = 30;
const UNKNOWN_ID: &str = "[unknown id]";

wit_bindgen_wasmtime::import!({paths: ["sqs.wit"], async: *});

pub(crate) type RuntimeData = sqs::SqsData;

type Command = TriggerExecutorCommand<SqsTrigger>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let t = Command::parse();
    t.run().await
}

pub struct SqsTrigger {
    engine: TriggerAppEngine<Self>,
    queue_components: Vec<Component>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqsTriggerConfig {
    pub component: String,
    pub queue_url: String,
    pub max_messages: Option<u32>,
    pub idle_wait_seconds: Option<u64>,
    pub system_attributes: Option<Vec<String>>,
    pub message_attributes: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
struct Component {
    pub id: String,
    pub queue_url: String,
    pub max_messages: i32,  // Should be usize but AWS
    pub idle_wait: tokio::time::Duration,
    pub system_attributes: Vec<aws_sdk_sqs::model::QueueAttributeName>,
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

    fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let queue_components = engine
            .trigger_configs()
            .map(|(_, config)| Component {
                id: config.component.clone(),
                queue_url: config.queue_url.clone(),
                max_messages: config.max_messages.unwrap_or(10).try_into().unwrap(),   // TODO: HA HA HA... YES!!!
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

        let client = aws_sdk_sqs::Client::new(&config);
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
    fn start_receive_loop(engine: Arc<TriggerAppEngine<Self>>, client: &aws_sdk_sqs::Client, component: &Component) -> tokio::task::JoinHandle<TerminationReason> {
        let future = Self::receive(engine, client.clone(), component.clone());
        tokio::task::spawn(future)
    }

    // This doesn't return a Result because we don't want a thoughtless `?` to exit the loop
    // and terminate the entire trigger.  Termination should be a conscious decision when
    // we are sure there is no point continuing.
    async fn receive(engine: Arc<TriggerAppEngine<Self>>, client: aws_sdk_sqs::Client, component: Component) -> TerminationReason {
        let queue_timeout_secs = get_queue_timeout_secs(&client, &component.queue_url).await;

        loop {
            tracing::trace!("Queue {}: attempting to receive up to {}", component.queue_url, component.max_messages);

            let rmo = match client
                .receive_message()
                .queue_url(&component.queue_url)
                .max_number_of_messages(component.max_messages)
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
                for m in msgs {
                    // Spin off the execution so it doesn't block the queue
                    let e = engine.clone();
                    let cl = client.clone();
                    let comp = component.clone();
                    tokio::spawn(async move { Self::process_message(m, e, cl, comp, queue_timeout_secs).await; });
                }
            } else {
                tracing::trace!("Queue {}: no messages received", component.queue_url);
                tokio::time::sleep(component.idle_wait).await;
            }
        }
    }

    async fn execute(engine: &Arc<TriggerAppEngine<Self>>, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
        let msg_id = message.id.unwrap_or(UNKNOWN_ID).to_owned();
        tracing::trace!("Message {msg_id}: executing component {component_id}");
        let (instance, mut store) = engine.prepare_instance(component_id).await?;
        let sqs_engine = Sqs::new(&mut store, &instance, |data| data.as_mut())?;
        match sqs_engine.handle_queue_message(&mut store, message).await {
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

    async fn process_message(m: aws_sdk_sqs::model::Message, engine: Arc<TriggerAppEngine<Self>>, client: aws_sdk_sqs::Client, component: Component, queue_timeout_secs: u16) {
        let msg_id = m.message_id().unwrap_or(UNKNOWN_ID).to_owned();
        tracing::trace!("Message {msg_id}: spawned processing task");

        // The attr lists have to be returned to this level so that they live long enough
        let attrs = to_wit_message_attrs(&m);
        let message = sqs::Message {
            id: m.message_id(),
            message_attributes: &attrs,
            body: m.body(),
        };

        let renew_lease = hold_message_lease(&client, &component, &m, queue_timeout_secs);

        let action = Self::execute(&engine, &component.id, message).await;

        if let Some(renewer) = renew_lease {
            renewer.abort();
        }

        match action {
            Ok(sqs::MessageAction::Delete) => {
                tracing::trace!("Message {msg_id} processed successfully: action is Delete");
                if let Some(receipt_handle) = m.receipt_handle() {
                    tracing::trace!("Message {msg_id}: attempting to delete via {receipt_handle}");
                    match client.delete_message().queue_url(&component.queue_url).receipt_handle(receipt_handle).send().await {
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
}

fn to_wit_message_attrs(m: &aws_sdk_sqs::model::Message) -> Vec<sqs::MessageAttribute> {
    let msg_id = m.message_id().unwrap_or(UNKNOWN_ID).to_owned();

    let sysattrs = m.attributes()
        .map(|a|
            a
            .iter()
            .map(|(k, v)| sqs::MessageAttribute { name: k.as_str(), value: sqs::MessageAttributeValue::Str(v.as_str()), data_type: None })
            .collect::<Vec<_>>()
        ).unwrap_or_default();
    let userattrs = m.message_attributes()
        .map(|a|
            a
            .iter()
            .filter_map(|(k, v)| {
                match wit_value(v) {
                    Ok(wv) => Some(sqs::MessageAttribute { name: k.as_str(), value: wv, data_type: None }),
                    Err(e) => {
                        tracing::error!("Message {msg_id}: can't convert attribute {} to string or blob, skipped: {e:?}", k.as_str());  // TODO: this should probably fail the message
                        None
                    },
                }
            })
            .collect::<Vec<_>>()
        ).unwrap_or_default();
    vec![sysattrs, userattrs].concat()
}

async fn get_queue_timeout_secs(client: &aws_sdk_sqs::Client, queue_url: &str) -> u16 {
    match client.get_queue_attributes().queue_url(queue_url).attribute_names(aws_sdk_sqs::model::QueueAttributeName::VisibilityTimeout).send().await {
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

fn hold_message_lease(client: &aws_sdk_sqs::Client, component: &Component, m: &aws_sdk_sqs::model::Message, timeout_secs: u16) -> Option<tokio::task::JoinHandle<()>> {
    let client = client.clone();
    let queue_url = component.queue_url.clone();
    let rcpt_handle = m.receipt_handle().map(|s| s.to_owned());
    let msg_id = m.message_id().unwrap_or("[unknown ID]").to_owned();
    let interval = tokio::time::Duration::from_secs((timeout_secs / 2).into());

    // TODO: is it worth figuring out a way to batch the renewals?  Same with delete
    // actions I guess
    rcpt_handle.map(|rh| tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            ticker.tick().await;
            tracing::info!("Message {msg_id}: renewing lease via {rh}");
            let cmv = client.change_message_visibility().queue_url(&queue_url).receipt_handle(&rh).visibility_timeout(timeout_secs.into()).send().await;
            if let Err(e) = cmv {
                tracing::error!("Message {msg_id}: failed to update lease: {}", e.to_string());
            }
        }
    }))
}

fn wit_value(v: &aws_sdk_sqs::model::MessageAttributeValue) -> Result<sqs::MessageAttributeValue> {
    if let Some(s) = v.string_value() {
        Ok(sqs::MessageAttributeValue::Str(s))
    } else if let Some(b) = v.binary_value() {
        Ok(sqs::MessageAttributeValue::Binary(b.as_ref()))
    } else {
        Err(anyhow::anyhow!("Don't know what to do with message attribute value {:?} (data type {:?})", v, v.data_type()))
    }
}
