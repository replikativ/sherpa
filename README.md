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

## License

Copyright © 2023 Konrad Kühne

You can create test data with

```bash
bb create-test-data 0.3.0 bb/resources/export-test-config.edn 100
```

Distributed under the Eclipse Public License version 1.0.
