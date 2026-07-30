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

**On protocol upgrades**, since tests use a specific one, you'll need to update these files accordingly. Usually there aren't any configuration changes between protocols, but sometimes there are. Here's a convenient command to rename the older protocol config files to the newer number, just substitute the numbers accordingly:

```bash
OLD=23 NEW=25; ls -1 *.p$OLD.* | awk '{printf $1 " " $1 "\n"}' | sed -E "s/p$OLD.(json|xdr)$/p$NEW.\1/" | xargs -n 2 mv $1 $2
```