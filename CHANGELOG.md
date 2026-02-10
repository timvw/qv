# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.16](https://github.com/timvw/qv/compare/v0.9.15...v0.9.16) - 2026-02-10

### Fixed

- *(renovate)* include major updates in datafusion ecosystem group ([#162](https://github.com/timvw/qv/pull/162))

### Other

- *(renovate)* group datafusion/deltalake/object_store deps together
- *(deps)* update rust docker tag to v1.93 ([#158](https://github.com/timvw/qv/pull/158))
- *(deps)* update github artifact actions (major) ([#155](https://github.com/timvw/qv/pull/155))
- *(deps)* update rust docker tag to v1.92 ([#154](https://github.com/timvw/qv/pull/154))
- Add explicit permissions to GitHub workflows
- Fix clippy warnings for Rust 1.91
- *(deps)* update rust docker tag to v1.91
- Enable GitHub auto-merge in settings
- Point release-plz at local manifest to skip crates.io version check
- *(deps)* update github artifact actions
- Reorder release flow to upload assets before creating release

## [0.9.15](https://github.com/timvw/qv/compare/v0.9.7...v0.9.15) - 2025-11-25

### Fixed

- *(deps)* update rust crate clap to 4.5.4 ([#119](https://github.com/timvw/qv/pull/119))
- *(deps)* update rust crate chrono to 0.4.38 ([#118](https://github.com/timvw/qv/pull/118))
- *(deps)* update rust crate aws-config to 1.2.1 ([#117](https://github.com/timvw/qv/pull/117))
- *(deps)* update rust crate aws-sdk-glue to 1.27 ([#115](https://github.com/timvw/qv/pull/115))
- *(deps)* update rust crate aws-sdk-glue to 1.26 ([#113](https://github.com/timvw/qv/pull/113))
- *(deps)* update aws-sdk-rust monorepo ([#112](https://github.com/timvw/qv/pull/112))
- *(deps)* update rust crate aws-sdk-glue to 1.24 ([#111](https://github.com/timvw/qv/pull/111))
- *(deps)* update rust crate aws-sdk-glue to 1.23

### Other

- Add release-plz automation (PR on main, tag on merged release PR)
- bump data stack and normalize local paths
- Install per-target toolchain in binaries workflows
- Simplify test command creation with cargo_bin!
- Import assert_cmd::cargo for cargo_bin!
- Use cargo::cargo_bin! macro in integration tests
- Place clippy allow at crate root
- Silence clippy result_large_err in integration tests
- Fix binaries-check workflow and silence clippy large Err lint
- Gate vendored OpenSSL per target and add binaries check workflow
- Use Strawberry Perl for Windows vendored OpenSSL
- Merge branch 'main' into renovate/mikepenz-action-junit-report-6.x
- *(deps)* update mikepenz/action-junit-report action to v6
- Vendor OpenSSL to fix cross builds
- Inline OpenSSL install command for cross
- Create release before uploading binaries
- Add settings to delete merged branches
- Pin delta_kernel to 0.6.0 for arrow 53
- Add OpenSSL install helper for cross targets
- Fix cross builds for Linux targets
- Switch Linux builds to cross
- Build OpenSSL for cross targets
- Fix Linux builds for release workflow
- Drop FreeBSD build from binary workflow
- Simplify release flow
- *(deps)* update mikepenz/action-junit-report action to v5
- *(deps)* update rust docker tag to v1.83 ([#126](https://github.com/timvw/qv/pull/126))
- *(deps)* update codecov/codecov-action action to v5
- make it work with our custom/patched version
- clippy
- fmt
- upgrade to latest versions
- *(deps)* update rust crate assert_cmd to 2.0.14 ([#116](https://github.com/timvw/qv/pull/116))

## [0.9.6](https://github.com/timvw/qv/compare/v0.9.5...v0.9.6) - 2024-03-30

### Other
- Merge branch 'main' into ci/attempt-to-build-binaries
- attempt to build binaries on release

## [0.9.5](https://github.com/timvw/qv/compare/v0.9.4...v0.9.5) - 2024-03-30

### Added
- add support for running on gcs
- Add support for gcs back

### Other
- allow creation of manual run
- one more attempt to trigger builds
- lint
- remove ref to mod
- remove more unused files
- lint
- remove unused files

## [0.9.4](https://github.com/timvw/qv/compare/v0.9.3...v0.9.4) - 2024-03-30

### Other
- build and publish more binaries upon release

## [0.9.3](https://github.com/timvw/qv/compare/v0.9.2...v0.9.3) - 2024-03-30

### Other
- attempt to triggers builds on release-plz mr/branch
- split tests

## [0.9.2](https://github.com/timvw/qv/compare/v0.9.1...v0.9.2) - 2024-03-30

### Added
- add test to verify that ndjson works

### Other
- allow pr builds for release-plz
- do not build with verbose flag
- provide code-cov token
- allow manual launch of test flow
- fmt
- add test to verify that gzipped json file is supported

## [0.9.1](https://github.com/timvw/qv/compare/v0.9.0...v0.9.1) - 2024-03-29

### Added
- make changes such that a glue deltalake table can be loaded
- add deltalake support again
- add badges to readme
- infer schema from glue catalog info
- add support for listing on s3 as well
- add support for listing files in a folder (also on s3)
- add support for aws s3 console url
- leverage rust aws sdk to get credentials
- leverage opendal instead of object_store features

### Fixed
- remove unwantend print
- change expected output
- add missing region for test

### Other
- lint
- *(deps)* update codecov/codecov-action action to v4 ([#89](https://github.com/timvw/qv/pull/89))
- attempt to add codecoverage
- attempt to trigger test run only once
- only annotate tests results
- do not group prs
- specify versions
- use nightly
- attempt to capture test results and upload them
- revert to datafusion 35 such that we can add the deltalake crate
- move things around
- *(deps)* update rust docker tag to v1.77 ([#69](https://github.com/timvw/qv/pull/69))
- add test to verify that s3 console url works
- improve the way we build expected output
- fmt
- more documentation on how aws s3 profiles work
- changes for gcs introduction
- add entry on s3 creds
- add entry on s3 creds
- trim expected output
- lint
- added entry on releases
- updated developer instructions
- start/stop minio before/after tests
- remove unused files
- change to tokio 1 to have latest
