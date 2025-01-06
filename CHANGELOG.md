# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.7](https://github.com/timvw/qv/compare/v0.9.6...v0.9.7) - 2025-01-06

### Fixed

- *(deps)* update rust crate clap to 4.5.4 (#119)
- *(deps)* update rust crate chrono to 0.4.38 (#118)
- *(deps)* update rust crate aws-config to 1.2.1 (#117)
- *(deps)* update rust crate aws-sdk-glue to 1.27 (#115)
- *(deps)* update rust crate aws-sdk-glue to 1.26 (#113)
- *(deps)* update aws-sdk-rust monorepo (#112)
- *(deps)* update rust crate aws-sdk-glue to 1.24 (#111)
- *(deps)* update rust crate aws-sdk-glue to 1.23

### Other

- *(deps)* update rust docker tag to v1.83 (#126)
- make it work with our custom/patched version
- clippy
- fmt
- upgrade to latest versions
- *(deps)* update rust crate assert_cmd to 2.0.14 (#116)

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
