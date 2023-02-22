use std::{collections::HashMap, sync::Arc};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use serde::{Deserialize, Serialize};
use sqs::Sqs;
use spin_trigger::{cli::{NoArgs, TriggerExecutorCommand}, TriggerAppEngine, TriggerExecutor};

// TODO: dynamically
const QUEUE_TIMEOUT_SECS: u16 = 30;

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
        let queue_timeout_secs = get_queue_timeout_secs(&client, &component.queue_url).await;

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
                let msgs = msgs.to_vec();
                println!("...received {} message(s) from {}", msgs.len(), component.queue_url);
                for m in msgs {
                    // Spin off the execution so it doesn't block the queue
                    let e = engine.clone();
                    let cl = client.clone();
                    let comp = component.clone();
                    tokio::spawn(async move { Self::process_message(m, e, cl, comp, queue_timeout_secs).await; });
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

    async fn process_message(m: aws_sdk_sqs::model::Message, engine: Arc<TriggerAppEngine<Self>>, client: aws_sdk_sqs::Client, component: Component, queue_timeout_secs: u16) {
        // This has to be inlined or the lists fall off the edge of the borrow checker
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

        let renew_lease = hold_message_lease(&client, &component, &m, queue_timeout_secs);

        let action = Self::execute(&engine, &component.id, message).await;
        println!("...action is to {action:?}");

        if let Some(renewer) = renew_lease {
            renewer.abort();
        }

        match action {
            Ok(sqs::MessageAction::Delete) => {
                if let Some(receipt_handle) = m.receipt_handle() {
                    match client.delete_message().queue_url(&component.queue_url).receipt_handle(receipt_handle).send().await {
                        Ok(_) => (),
                        Err(e) => eprintln!("TRIG: err deleting {receipt_handle}: {e:?}"),
                    }
                }
            }
            Ok(sqs::MessageAction::Leave) => (),  // TODO: change message visibility to 0
            Err(e) => {
                eprintln!("Error processing message {:?}: {}", m.message_id(), e.to_string())
                // TODO: change message visibility to 0 I guess?
            }
        }
   
    }
}

async fn get_queue_timeout_secs(client: &aws_sdk_sqs::Client, queue_url: &str) -> u16 {
    match client.get_queue_attributes().queue_url(queue_url).attribute_names(aws_sdk_sqs::model::QueueAttributeName::VisibilityTimeout).send().await {
        Err(e) => {
            eprintln!("Unable to establish queue timeout, using default {QUEUE_TIMEOUT_SECS} secs: {}", e.to_string());
            QUEUE_TIMEOUT_SECS
        },
        Ok(gqa) => {
            match gqa.attributes() {
                None => {
                        eprintln!("No attrs, using default {QUEUE_TIMEOUT_SECS} secs");
                        QUEUE_TIMEOUT_SECS
                }
                Some(attrs) => match attrs.get(&aws_sdk_sqs::model::QueueAttributeName::VisibilityTimeout) {
                    None => {
                        eprintln!("No timeout attr found, using default {QUEUE_TIMEOUT_SECS} secs");
                        QUEUE_TIMEOUT_SECS
                    },
                    Some(vt) => {
                        eprintln!("Parsing queue tiemout {vt}");
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
    let renew_lease = rcpt_handle.map(|rh| tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            ticker.tick().await;
            println!("Renewing lease for message id {msg_id}");
            let cmv = client.change_message_visibility().queue_url(&queue_url).receipt_handle(&rh).visibility_timeout(timeout_secs.into()).send().await;
            if let Err(e) = cmv {
                eprintln!("Failed to update lease for message id {msg_id}: {}", e.to_string());
            }
        }
    }));
    renew_lease
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
