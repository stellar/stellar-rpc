To change limits, install the CLI and `stellar-xdr`:

```bash
cargo install --locked stellar-cli@v23.0.0;
cargo install --locked stellar-xdr@v23.0.0-rc.1;
```

then convert the JSON to base64 for the file you changed, e.g.:

```bash
stellar xdr encode --type ConfigUpgradeSet < testnet-limits.json > testnet-limits.txt
```