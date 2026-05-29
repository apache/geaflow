# Apache GeaFlow (Incubating) Release Notes

## 0.8.0 — _planned_

> Release date: TBD
> Release manager: TBD

### Highlights

- _(to be filled in before vote)_

### Breaking Changes

- **MySQL JDBC driver is no longer bundled in convenience binaries.**
  The MySQL Connector/J dependency (`mysql:mysql-connector-java`) is licensed
  under GPLv2 with the FOSS / Classpath Exception, which is a
  [Category-X](https://www.apache.org/legal/resolved.html#category-x) license
  under the Apache Software Foundation policy and may not be redistributed
  as part of an Apache release.
  Starting from 0.8.0, the driver is declared with `<scope>provided</scope>`
  in `geaflow-store-jdbc` and `geaflow-console-common-dal`.
  **Action required for users:** when deploying `geaflow-console` or using
  the JDBC store with a MySQL backend, download a compatible MySQL JDBC
  driver and add it to the runtime classpath (e.g. drop the jar into
  Spring Boot's `--loader.path` or place it in the operator's `lib/`).

### Removed from Source Release

- **`data/InferUDF.zip`** has been removed from the source repository because
  it contained pre-trained binary model weights (`model.pt`) and IDE/macOS
  metadata, which are not source-form artifacts permitted in an Apache
  source release. The reference inference UDF project can be obtained from
  the project download page or built by following the directory structure
  documented in `docs/docs-{cn,en}/source/3.quick_start/3.quick_start_infer&UDF.md`.

### Not Included in This Release

- **`geaflow-kubernetes-operator`** follows an independent release cadence
  (currently `0.4.0-SNAPSHOT`) and is excluded from the GeaFlow 0.8.0 source
  release tarball via `.gitattributes` `export-ignore`. Refer to the
  operator project's own release notes for its versioning.

### Features

- _(to be filled in)_

### Improvements

- _(to be filled in)_

### Bug Fixes

- _(to be filled in)_

### Dependency Upgrades

- _(to be filled in)_

### Contributors

- _(to be filled in before vote — generate via `git shortlog -sn v0.7.0..v0.8.0`)_
