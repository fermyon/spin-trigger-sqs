```
cargo build --release
spin pluginify
spin plugins install --file trigger-sqs.json --yes

cd guest
spin build --up --follow-all
```
