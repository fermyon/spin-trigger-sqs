# Experimental SQS trigger for Spin

## Install from release

```
spin plugins install --url https://github.com/fermyon/spin-trigger-sqs/releases/download/canary/trigger-sqs.json
```

## Build from source

```
cargo build --release
spin pluginify
spin plugins install --file trigger-sqs.json --yes
```

## Test

The trigger uses the AWS configuration environment variables - these must be set before running.

```
cd guest
spin build --up
```

## Configuration

Documentation is TBD but see `guest/spin.toml` for an example of the configuration knobs.
