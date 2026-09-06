# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.1](https://github.com/timvw/qv/compare/v0.10.0...v0.10.1) - 2026-09-06

### Added

- upgrade data stack and add Iceberg support
- add support for running on gcs
- Add support for gcs back
- add test to verify that ndjson works
- make changes such that a glue deltalake table can be loaded
- add deltalake support again
- add badges to readme
- infer schema from glue catalog info
- add support for listing on s3 as well
- add support for listing files in a folder (also on s3)
- add support for aws s3 console url
- leverage rust aws sdk to get credentials
- leverage opendal instead of object_store features
- exclude dev resources from package
- add test for listing multiple parquet files
- apply limit on non-schema queries
- add support for google cloud storage
- allow user to specify AWS_PROFILE as an optional parameter (--profile)
- implement globbing

### Fixed

- address automated review feedback
- *(renovate)* include major updates in datafusion ecosystem group ([#162](https://github.com/timvw/qv/pull/162))
- *(deps)* update rust crate clap to 4.5.4 ([#119](https://github.com/timvw/qv/pull/119))
- *(deps)* update rust crate chrono to 0.4.38 ([#118](https://github.com/timvw/qv/pull/118))
- *(deps)* update rust crate aws-config to 1.2.1 ([#117](https://github.com/timvw/qv/pull/117))
- *(deps)* update rust crate aws-sdk-glue to 1.27 ([#115](https://github.com/timvw/qv/pull/115))
- *(deps)* update rust crate aws-sdk-glue to 1.26 ([#113](https://github.com/timvw/qv/pull/113))
- *(deps)* update aws-sdk-rust monorepo ([#112](https://github.com/timvw/qv/pull/112))
- *(deps)* update rust crate aws-sdk-glue to 1.24 ([#111](https://github.com/timvw/qv/pull/111))
- *(deps)* update rust crate aws-sdk-glue to 1.23
- remove unwantend print
- change expected output
- add missing region for test
- fix clippy issue
- *(deps)* update all dependencies ([#68](https://github.com/timvw/qv/pull/68))
- *(deps)* update rust crate clap to 4.1.0 ([#67](https://github.com/timvw/qv/pull/67))
- *(deps)* update rust crate regex to 1.7.1 ([#65](https://github.com/timvw/qv/pull/65))
- *(deps)* update all dependencies ([#64](https://github.com/timvw/qv/pull/64))
- *(deps)* update rust crate clap to 4.0.29 ([#62](https://github.com/timvw/qv/pull/62))
- *(deps)* update rust crate clap to 4.0.28 ([#61](https://github.com/timvw/qv/pull/61))
- *(deps)* update rust crate clap to 4.0.27 ([#60](https://github.com/timvw/qv/pull/60))
- *(deps)* update rust crate tokio to 1.22 ([#59](https://github.com/timvw/qv/pull/59))
- *(deps)* update rust crate clap to 4.0.26 ([#58](https://github.com/timvw/qv/pull/58))
- *(deps)* update rust crate clap to 4.0.25 ([#57](https://github.com/timvw/qv/pull/57))
- *(deps)* update rust crate clap to 4.0.24 ([#56](https://github.com/timvw/qv/pull/56))
- *(deps)* update rust crate chrono to 0.4.23 ([#54](https://github.com/timvw/qv/pull/54))
- *(deps)* update rust crate clap to 4.0.23 ([#53](https://github.com/timvw/qv/pull/53))
- *(deps)* update rust crate clap to 4.0.22 ([#51](https://github.com/timvw/qv/pull/51))
- fix test
- *(deps)* update rust crate clap to 4.0.20 ([#50](https://github.com/timvw/qv/pull/50))
- fix volume param
- fix volume param
- fix typo
- fix gc name
- fix linting issue on unwrap
- *(deps)* update rust crate clap to 4.0.11 ([#34](https://github.com/timvw/qv/pull/34))
- *(deps)* update rust crate clap to 4.0.10 ([#33](https://github.com/timvw/qv/pull/33))
- *(deps)* update rust crate clap to 4.0.9 ([#32](https://github.com/timvw/qv/pull/32))
- *(deps)* update rust crate clap to 4.0.8 ([#29](https://github.com/timvw/qv/pull/29))
- discovery of delta tables
- *(deps)* update all dependencies to 0.49
- ensure that single file can also be queries
- corrected codepath in case no globbing character is provided

### Other

- prepare v0.10.0 release
- release v0.9.16
- exercise Iceberg REST catalog end to end
- clarify S3 console URL support
- explain Storj S3 gateway configuration
- *(deps)* update rust docker tag to v1.98 ([#174](https://github.com/timvw/qv/pull/174))
- *(deps)* update rust docker tag to v1.97 ([#173](https://github.com/timvw/qv/pull/173))
- *(deps)* update actions/checkout action to v7 ([#172](https://github.com/timvw/qv/pull/172))
- *(deps)* update codecov/codecov-action action to v7 ([#171](https://github.com/timvw/qv/pull/171))
- *(deps)* update rust docker tag to v1.96 ([#170](https://github.com/timvw/qv/pull/170))
- *(deps)* update rust docker tag to v1.95 ([#169](https://github.com/timvw/qv/pull/169))
- *(deps)* update softprops/action-gh-release action to v3 ([#168](https://github.com/timvw/qv/pull/168))
- *(deps)* update codecov/codecov-action action to v6 ([#167](https://github.com/timvw/qv/pull/167))
- *(deps)* update actions/create-github-app-token action to v3 ([#166](https://github.com/timvw/qv/pull/166))
- *(deps)* update rust docker tag to v1.94 ([#165](https://github.com/timvw/qv/pull/165))
- *(deps)* update github artifact actions (major) ([#164](https://github.com/timvw/qv/pull/164))
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
- Update changelog for 0.9.15
- Bump version to 0.9.15
- release v0.9.7
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
- release
- Merge branch 'main' into ci/attempt-to-build-binaries
- attempt to build binaries on release
- allow creation of manual run
- one more attempt to trigger builds
- lint
- remove ref to mod
- remove more unused files
- lint
- remove unused files
- release
- build and publish more binaries upon release
- release
- attempt to triggers builds on release-plz mr/branch
- split tests
- release
- allow pr builds for release-plz
- do not build with verbose flag
- provide code-cov token
- allow manual launch of test flow
- fmt
- add test to verify that gzipped json file is supported
- release
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
- update version to 0.9.0
- disable test for now
- also checkout git submodules for test data
- use integration tests
- attempt to correctly get binary
- enable all features tst
- disable legacy workflows
- disable warn on unused code
- enable tests again
- new api
- bump versions (wip)
- add linting and run simple cargo test
- add pre-commit config
- add release-plz workflows
- reduce number of architectures
- bump base docker images to more recent rust
- Do not (try to) publish on crates.io anymore as the binary is too large..
- use more recent runner
- migrate away from set-output.. https://github.blog/changelog/2022-10-11-github-actions-deprecating-save-state-and-set-output-commands/
- added links for csv and ndjson format
- clippy fixes
- cargo fmt
- all tests passing
- initial work to bump to delta 0.12  (2 tests failing due to changes in output)
- *(deps)* update rust crate assert_cmd to 2.0.8 ([#66](https://github.com/timvw/qv/pull/66))
- cleanup imports
- leverage aws_profile from object_store
- remove iceberg and bump datafusion and delta
- bump object_store to 0.5.3
- *(deps)* update all dependencies ([#63](https://github.com/timvw/qv/pull/63))
- *(deps)* update rust crate predicates to 2.1.3 ([#55](https://github.com/timvw/qv/pull/55))
- bump to df14
- updated readme
- use ci script
- try to move all invocations into scripts (such that they can be re-used by a regular dev)
- wip
- Merge branch 'main' into betterci
- wip Sat Nov  5 22:04:42 CET 2022
- wip Sat Nov  5 21:56:20 CET 2022
- wip Sat Nov  5 21:55:27 CET 2022
- wip Sat Nov  5 21:52:12 CET 2022
- wip Sat Nov  5 21:52:04 CET 2022
- wip Sat Nov  5 21:47:25 CET 2022
- wip Sat Nov  5 21:45:25 CET 2022
- wip Sat Nov  5 21:42:33 CET 2022
- wip Sat Nov  5 21:41:12 CET 2022
- wip Sat Nov  5 21:36:49 CET 2022
- wip Sat Nov  5 21:34:15 CET 2022
- wip Sat Nov  5 21:31:28 CET 2022
- wip Sat Nov  5 21:30:44 CET 2022
- wip Sat Nov  5 21:20:19 CET 2022
- wip Sat Nov  5 21:20:00 CET 2022
- wip Sat Nov  5 21:07:48 CET 2022
- wip Sat Nov  5 21:05:57 CET 2022
- wip Sat Nov  5 21:01:16 CET 2022
- wip Sat Nov  5 20:58:15 CET 2022
- wip Sat Nov  5 20:52:53 CET 2022
- wip Sat Nov  5 20:52:47 CET 2022
- wip Sat Nov  5 20:35:09 CET 2022
- wip Sat Nov  5 20:34:08 CET 2022
- wip Sat Nov  5 20:26:51 CET 2022
- wip Sat Nov  5 20:16:19 CET 2022
- wip Sat Nov  5 19:40:57 CET 2022
- wip Sat Nov  5 19:39:34 CET 2022
- wip Sat Nov  5 19:38:21 CET 2022
- wip Sat Nov  5 19:36:18 CET 2022
- wip Sat Nov  5 19:31:17 CET 2022
- wip Sat Nov  5 19:29:19 CET 2022
- wip Sat Nov  5 19:27:47 CET 2022
- wip Sat Nov  5 19:24:53 CET 2022
- wip Sat Nov  5 19:15:24 CET 2022
- wip Sat Nov  5 19:14:15 CET 2022
- wip Sat Nov  5 19:12:06 CET 2022
- remove tests
- do not load aws sdk when not required
- added additional -- for args
- added steps to capture test results and publish
- another attempt
- output xml
- updated ci workflow
- ignore tarpaulin output
- disable some tests for now
- remove ,
- remove useless borrow
- make it work on minio
- only provide the location/prefix of the table, excluding the object-store scheme and bucketname
- add support for s3a and ensure that aws_default_region is set
- help the compiler infer the desired type
- lint
- updated readme
- load iceberg table when metadata folder is present
- add dependency on iceberg
- *(deps)* update enricomi/publish-unit-test-result-action action to v2 ([#42](https://github.com/timvw/qv/pull/42))
- disable codecoverage
- another attempt at codecov
- ignore more codecoverage artifacts
- upload code coverage results
- ignore code coverage output
- Merge branch 'main' into test
- simple
- try to upload test/code coverage
- use action-rs for cargo invocation
- checkout submodules as well
- another attempt to set path for test data
- another attempt to set path for test data
- try to ru tests
- enable fmt and clippy again
- only build for now
- added caching step
- another attempt to set QV_TESTING_PATH
- changes to make the tests pass again
- bump to datafusion 13/arrow 24
- added another test for local parquet
- build before running test
- remove newlines
- initial test to verify main behavior
- avoid issues with rusoto and do not use default features in delta-rs
- Improve README.md styling
- ISSUE-30 Update installation instructions and update version number to latest release (0.4.0)
- *(deps)* update all dependencies ([#24](https://github.com/timvw/qv/pull/24))
- cleanup + lint
- do not run builds twice
- comment test which does not work on CI
- updated testing module to have a folder with a delta table
- remove existing test data
- added additional test to verify that a folder with csv file is not considered a delta table folder
- force the creation of a PR when bumping the version
- do not build container as ci anymore
- added test to reproduce and fix issue
- combine cargo test, fmt and clippy in single job
- rework test workflow
- updated publishing of artifacts
- update code to not filter hidden folders, because _delta_log is hidden
- run lint
- simplify logic to determine what kind of table to load
- update readme with usage for glue
- add support for glue tables
- Merge branch 'main' into renovate/all
- *(deps)* update rust docker tag to v1.64
- rework flow to build container not in matrix
- added testbuild
- run lint
- update case to always consider scheme and host for object_store_url
- remove unused workflow
- build on each push (but do not add to release)
- remove more unused deps
- bump version number
- remove dependency on anyhow
- added some tests to verify is_hidden
- smal cleanup of load listing table
- further refactor into modules
- move handling of defaults and parsing/validating of data into Args
- refactor in more distinct components
- major refactoring in functions
- refactor into re-usable functions
- cleanup code
- rework logic to list files
- implement ensure_scheme
- better support for non-s3 object_stores
- updated README to inform users that they can use the --profile parameter (simliar to AWS cli)
- *(deps)* update all dependencies
- updated readme to publicize globbing support.
- ignore hidden files
- refactoring creation of listing_table in separate method
- further cleanup into functions
- updated readme
- further cleanup
- run cargo fmt
- cleanup some unused things
- rework such that delta tables are supported again
- initial work to bump to datafusion 12
- Build on older Ubuntu for Centos 8 (glibc 2.28) compatibility
- correct some casings
- updated installation instructions
- mention support for https links in aws s3 console as source
- another removal of incorrect (only bash) language on code snippet
- update some code snippets to not have bash(only) formatting
- updated README to expose feature of using aws console url
- add support for links from aws console
- update dependency to point to git repo
- added usage note for delta/s3
- added entry for deltalake support
- also support local files
- slight refactor to allow for other object_Stores as well
- working for some thingie
- initial work to support delta
- update github workflows
- bump to latest s3 objectstore
- set dep on dos-s3 correct
- rework usage
- make path the default parameter
- Update Rust crate tokio to 1.19
- updated install instructions for mac/homebrew
- disable gnu windows build
- do update formula. renovatebot is not good at picking correct binary
- set version back to 0.1.0
- added release workflow
- remove some workflows and replace wiht generic release workflow
- more experiment with release build
- add artiaacts whena  release is published instead of created
- remove file that does not exist
- rename thingies
- added newrelease wf
- added docker registry
- do not copy cargo.lock
- remove unused action
- wip docker build
- specify target path
- set ctx in docker build
- checkout code
- docker build
- make workable docker file
- try to push container ot ghcr.io
- add more options to install the app
- allow user to specify a custom query.. and change the number of returned rows
- depend on datafusion objectstore main branch again
- another attempt with matrix
- add container
- bleh
- specify arch and rust types
- generate tar.gz for macos
- rework workflows
- Update Rust crate tokio to 1.18
- rename workflow to Release
- added release build
- bump to version to 0.2.0
- added support for s3
- updated README with installation instruction
- ammend readme
- reduce numer of keywords
- extend package
- updated readme
- rename to qv
- added some sample usages
- enable avro feature
- added testing submodule
- linting
- added lint script
- ignore .idea
- added github config
- intial commit. works with a local csv file
- Initial commit

## [0.10.0](https://github.com/timvw/qv/compare/v0.9.15...v0.10.0) - 2026-09-06

### Added

- Read Apache Iceberg tables from local storage, S3, and GCS using their snapshot metadata.
- Load Iceberg tables from AWS Glue and Iceberg REST catalogs.
- Add Iceberg catalog/storage properties, environment-backed secrets, and timestamp-based snapshot selection.

### Changed

- Upgrade the data stack to DataFusion 53.1, Arrow 58, Delta Lake 0.32.4, and Iceberg 0.10.1.
- Commit `Cargo.lock` for reproducible qv binary builds.
- Use `AWS_ENDPOINT_URL_GLUE` and `AWS_ENDPOINT_URL_S3` for service-specific endpoints; the global `AWS_ENDPOINT_URL` now applies only as an S3 fallback.

### Documentation

- Clarify S3 console URL support and document the Storj S3 gateway configuration.

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
