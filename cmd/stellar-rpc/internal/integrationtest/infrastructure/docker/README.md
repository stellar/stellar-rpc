To change the limits in [`upgrades/`](./upgrades/), install the CLI and `stellar-xdr`:

```bash
cargo install --locked stellar-cli@v23.0.0;
cargo install --locked stellar-xdr@v23.0.0;
```

then convert the JSON to base64 for the file you changed, e.g.:

```bash
stellar xdr encode --type ConfigUpgradeSet < testnet.p23.json > testnet.p23.xdr
```