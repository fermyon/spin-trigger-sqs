
use clap::Parser;
use is_terminal::IsTerminal;
use trigger_sqs::SqsTrigger;
use spin_trigger::cli::TriggerExecutorCommand;

type Command = TriggerExecutorCommand<SqsTrigger>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let t = Command::parse();
    t.run().await
}