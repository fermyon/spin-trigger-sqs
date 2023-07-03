# Experimental SQS trigger for Spin

## Install from release

```
spin plugins install --url https://github.com/fermyon/spin-trigger-sqs/releases/download/canary/trigger-sqs.json
```

## Build from source

You will need Rust and the `pluginify` plugin (`spin plugins install --url https://github.com/itowlson/spin-pluginify/releases/download/canary/pluginify.json`).

```
cargo build --release
spin pluginify --install
```

## Test

The trigger uses the AWS configuration environment variables - these must be set before running.

You will also need to change the `queue_url` in `spin.toml` to a queue you have access to.

```
cd guest
spin build --up
```

## Limitations

This trigger is currently built using Spin 0.10, which is quite an old version and does not support recent Spin features such as key-value storage.  You will likely need to use an old version of the Spin SDK for building applications.

Custom triggers, such as this one, can be run in the Spin command line, but cannot be deployed to Fermyon Cloud.  For other hosts, check the documentation.

## Configuration

The SQS trigger uses the AWS credentials from the standard AWS configuration environment variables.  These variables must be set before you run `spin up`.  The credentials must grant access to all queues that the application wants to monitor.  The credentials must allow for reading messages and deleting read messages.

The trigger assumes that the monitored queues exist: it does not create them.

### `spin.toml` - application level

The trigger type is `sqs`:

```toml
spin_version = "1"
name = "test"
version = "0.1.0"
authors = ["itowlson <ivan.towlson@fermyon.com>"]
# This line sets the trigger type
trigger = { type = "sqs" }
```

There are no application-level configuration options.

### `spin.toml` - component level

The following options are available to set in the `[component.trigger]` section:

| Name                  | Type             | Required? | Description |
|-----------------------|------------------|-----------|-------------|
| `queue_url`           | string           | required | The queue to which this component listens and responds. |
| `max_messages`        | number           | optional | The maximum number of messages to fetch per AWS queue request. The default is 10. This refers specifically to how messages are fetched from AWS - the component is still invoked separately for each message. |
| `idle_wait_seconds`   | number           | optional | How long (in seconds) to wait between checks when the queue is idle (i.e. when no messages were received on the last check). The default is 2. If the queue is _not_ idle, there is no wait between checks. The idle wait is also applied if an error occurs. |
| `system_attributes`   | array of strings | optional | The list of system-defined [attributes](https://docs.rs/aws-sdk-sqs/latest/aws_sdk_sqs/operation/receive_message/builders/struct.ReceiveMessageFluentBuilder.html#method.set_attribute_names) to fetch and make available to the component. |
| `message_attributes`  | array of strings | optional | The list of [message attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html) to fetch and make available to the component. Only string and binary values are supported. |

### `spin up` command line options

There are no custom command line options for this trigger.

## Writing SQS components

There is no SDK for SQS guest components.  Use the `sqs.wit` file to generate a trigger binding for your language.  Your Wasm component must _export_ the `handle-queue-message` function.  See `guest/src/lib.rs`  for how to do this in Rust.

**Note:** In the current WIT, a message has a single `message-attributes` field. This contains both system and message attributes. Feedback is welcome on this design decision.

Your handler must return a `message-action` (or an error).  The `message-action` values are:

| Name       | Description |
|------------|-------------|
| `delete`   | The message has been processed and should be removed from the queue. |
| `leave`    | The message should be kept on the queue. |

The trigger renews the message lease for as long as the handler is running.

**Note:** The current trigger implementation does not make the message visible immediately if the handler returns `leave` or an error; it lets the message become visible through the normal visibility timeout mechanism.
