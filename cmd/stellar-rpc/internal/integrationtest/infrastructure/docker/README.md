To change the limits in [`upgrades/`](./upgrades/), install the CLI and `stellar-xdr`, e.g.:

```bash
cargo install --locked stellar-cli@v23.0.0;
cargo install --locked stellar-xdr@v23.0.0;
```

then convert the JSON to base64 for the file you changed, e.g.:

```bash
stellar xdr encode --type ConfigUpgradeSet < testnet.p23.json > testnet.p23.xdr
```

These files come from [quickstart](https://github.com/stellar/quickstart/tree/main/local/core/etc/config-settings) with a change to make them match the `TESTING_MINIMUM_PERSISTENT_ENTRY_LIFETIME=10` Core configuration (see `"min_persistent_ttl"`).

You can also create new files for certain test cases, like if you want state archival to kick in more aggressively. Just copy the file closest to what you need and follow the steps above, then set `ApplyLimits` in your integration `TestConfig` to that filename. It should be named `<name>.p<protocol version>.xdr`, but only `<name>` should be passed to `ApplyLimits`.