# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.1](https://github.com/timvw/qv/compare/v0.9.0...v0.9.1) - 2024-03-27

### Added
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
