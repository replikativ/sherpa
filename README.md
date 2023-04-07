# Sherpa

A trusted companion to travel your data sets between Datahike databases.

## Usage
The migration with bb is structured like the following:
```bash
bb migration <source-version> <path-to-source-cfg> <path-to-target-cfg>
```
for example:
```bash
bb migrate 0.3.0 bb/resources/export-test-config.edn bb/resources/import-test-config.edn
```

You can create test data with 1K datoms per transaction:
```bash
bb create-test-data <version> <config> <tx-count>

```

```bash
bb create-test-data 0.3.0 bb/resources/export-test-config.edn 100
```

## License

Copyright © 2023 Konrad Kühne

Distributed under the Eclipse Public License version 1.0.
