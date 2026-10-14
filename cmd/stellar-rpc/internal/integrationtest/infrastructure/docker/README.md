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

You can also create new files for specific test cases. For example, you can make state archival start sooner. Copy the file that is closest to what you need and do the steps above. Then set `ApplyLimits` in your integration `TestConfig` to the base name of the file. Name the file `<name>.p<protocol version>.xdr`. Pass only `<name>` to `ApplyLimits`.

**On protocol upgrades**, you usually do not need to change these files. For a test at protocol `N`, the test framework uses the `<name>.p<K>.xdr` file with the highest `K` that is not greater than `N`. For example, a P29 test uses `unlimited.p28.xdr` if `unlimited.p29.xdr` does not exist. Add a new `<name>.p<N>.xdr` file only when the config XDR changes in protocol `N`. Keep the older files while CI still tests the older protocols.
