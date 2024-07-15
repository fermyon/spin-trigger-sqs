# Experimental SQS trigger for Spin

## Install the latest version

The latest stable release of the SQS trigger plugin can be installed like so:

```sh
spin plugins update
spin plugin install trigger-sqs
```

## Install the canary version

The canary release of the SQS trigger plugin represents the most recent commits on `main` and may not be stable, with some features still in progress.

```sh
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
Be sure to set `AWS_DEFAULT_REGION` in your environment to the region of your queue.

You will also need to change the `queue_url` in `spin.toml` to a queue you have access to.

```
cd guest
spin build --up
```

## Limitations

This trigger is currently built using Spin 2.0.1. You will need that version of Spin or above.

Custom triggers, such as this one, can be run in the Spin command line, but cannot be deployed to Fermyon Cloud.  For other hosts, check the documentation.

## Configuration

The SQS trigger uses the AWS credentials from the standard AWS configuration environment variables.  These variables must be set before you run `spin up`.  The credentials must grant access to all queues that the application wants to monitor.  The credentials must allow for reading messages and deleting read messages.

The trigger assumes that the monitored queues exist: it does not create them.

### `spin.toml`

The trigger type is `sqs`, and there are no application-level configuration options.

The following options are available to set in the `[[trigger.sqs]]` section:

| Name                  | Type             | Required? | Description |
|-----------------------|------------------|-----------|-------------|
| `queue_url`           | string           | required | The queue to which this trigger listens and responds. |
| `max_messages`        | number           | optional | The maximum number of messages to fetch per AWS queue request. The default is 10. This refers specifically to how messages are fetched from AWS - the component is still invoked separately for each message. |
| `idle_wait_seconds`   | number           | optional | How long (in seconds) to wait between checks when the queue is idle (i.e. when no messages were received on the last check). The default is 2. If the queue is _not_ idle, there is no wait between checks. The idle wait is also applied if an error occurs. |
| `system_attributes`   | array of strings | optional | The list of system-defined [attributes](https://docs.rs/aws-sdk-sqs/latest/aws_sdk_sqs/operation/receive_message/builders/struct.ReceiveMessageFluentBuilder.html#method.set_attribute_names) to fetch and make available to the component. |
| `message_attributes`  | array of strings | optional | The list of [message attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html) to fetch and make available to the component. Only string and binary values are supported. |
| `component`           | string or table  | required | The component to run when a queue message is received. (This is the standard Spin trigger component field.) |

For example:

```toml
spin_manifest_version = 2

[application]
name = "test"
version = "0.1.0"

# One [[trigger.sqs]] section for each queue to monisot1
[[trigger.sqs]]
queue_url = "https://sqs.us-west-2.amazonaws.com/12345/testqueue"
max_messages = 1
system_attributes = ["All"]
component = "test"

[component.test]
source = "..."
```

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
