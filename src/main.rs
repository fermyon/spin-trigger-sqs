use std::{collections::HashMap, sync::Arc};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use serde::{Deserialize, Serialize};
use sqs::Sqs;
use spin_trigger::{cli::{NoArgs, TriggerExecutorCommand}, TriggerAppEngine, TriggerExecutor};

wit_bindgen_wasmtime::import!({paths: ["sqs.wit"], async: *});

pub(crate) type RuntimeData = sqs::SqsData;

type Command = TriggerExecutorCommand<SqsTrigger>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let t = Command::parse();
    t.run().await
}

pub struct SqsTrigger {
    engine: TriggerAppEngine<Self>,
    queue_components: Vec<Component>, // HashMap<String, String>,  // Queue URL -> component ID
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

        let (r, _, rest) = futures::future::select_all(loops).await;
        drop(rest);

        r?
    }
}

impl SqsTrigger {
    // TODO: would this work better returning a stream to allow easy multiplexing etc?
    fn start_receive_loop(engine: Arc<TriggerAppEngine<Self>>, client: &aws_sdk_sqs::Client, component: &Component) -> tokio::task::JoinHandle<Result<()>> {
        let future = Self::receive(engine, client.clone(), component.clone());
        tokio::task::spawn(future)
    }

    async fn receive(engine: Arc<TriggerAppEngine<Self>>, client: aws_sdk_sqs::Client, component: Component) -> Result<()> {
        loop {
            println!("Attempting to receive up to {} from {}...", component.max_messages, component.queue_url);

            let rmo = client
                .receive_message()
                .queue_url(&component.queue_url)
                .max_number_of_messages(component.max_messages)
                .set_attribute_names(Some(component.system_attributes.clone()))
                .set_message_attribute_names(Some(component.message_attributes.clone()))
                .send()
                .await?;

            if let Some(msgs) = rmo.messages() {
                println!("...received {} message(s) from {}", msgs.len(), component.queue_url);
                for m in msgs {
                    let empty = HashMap::new();
                    let empty2 = HashMap::new();
                    let sysattrs = m.attributes()
                        .unwrap_or(&empty)
                        .iter()
                        .map(|(k, v)| sqs::MessageAttribute { name: k.as_str(), value: sqs::MessageAttributeValue::Str(v.as_str()), data_type: None })
                        .collect::<Vec<_>>();
                    let userattrs = m.message_attributes()
                        .unwrap_or(&empty2)
                        .iter()
                        .map(|(k, v)| sqs::MessageAttribute { name: k.as_str(), value: wit_value(v), data_type: None })
                        .collect::<Vec<_>>();
                    let attrs = vec![sysattrs, userattrs].concat();
                    let message = sqs::Message {
                        id: m.message_id(),
                        message_attributes: &attrs,
                        body: m.body(),
                    };
                    //
                    // TODO: !!! HOLD THE LEASE WHILE PROCESSING !!!
                    //
                    let action = Self::execute(&engine, &component.id, message).await?;
                    println!("...action is to {action:?}");
                    if action == sqs::MessageAction::Delete {
                        if let Some(receipt_handle) = m.receipt_handle() {
                            match client.delete_message().queue_url(&component.queue_url).receipt_handle(receipt_handle).send().await {
                                Ok(_) => (),
                                Err(e) => eprintln!("TRIG: err deleting {receipt_handle}: {e:?}"),
                            }
                        }
                    }
                }
            } else {
                tokio::time::sleep(component.idle_wait).await;
            }
        }
    }

    async fn execute(engine: &Arc<TriggerAppEngine<Self>>, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
        println!("Executing component {component_id}");
        let (instance, mut store) = engine.prepare_instance(component_id).await?;
        let sqs_engine = Sqs::new(&mut store, &instance, |data| data.as_mut())?;
        match sqs_engine.handle_queue_message(&mut store, message).await {
            Ok(Ok(action)) => Ok(action),
            // TODO: BUTTLOAD OF LOGGING
            // TODO: DETECT FATALNESS
            Ok(Err(_e)) => Ok(sqs::MessageAction::Leave),
            Err(_e) => Ok(sqs::MessageAction::Leave),
        }
    }
}

fn wit_value(v: &aws_sdk_sqs::model::MessageAttributeValue) -> sqs::MessageAttributeValue {
    if let Some(s) = v.string_value() {
        sqs::MessageAttributeValue::Str(s)
    } else if let Some(b) = v.binary_value() {
        sqs::MessageAttributeValue::Binary(b.as_ref())
    } else {
        panic!("Don't know what to do with message attribute value {:?}", v);
    }
}
