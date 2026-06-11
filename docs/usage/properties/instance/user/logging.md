## Instance Properties - Logging - User Defined

The following instance properties relate to logging.

| Property Name                 | Description                                                                                                          | Default Value | Run CDK Deploy When Changed |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------|
| sleeper.logging.level         | The logging level for logging Sleeper classes. This does not apply to the MetricsLogger which is always set to INFO. |               | true                        |
| sleeper.logging.level.apache  | The logging level for Apache logs that are not Parquet.                                                              |               | true                        |
| sleeper.logging.level.parquet | The logging level for Parquet logs.                                                                                  |               | true                        |
| sleeper.logging.level.aws     | The logging level for AWS logs.                                                                                      |               | true                        |
| sleeper.logging.level.root    | The logging level for everything else.                                                                               |               | true                        |
| sleeper.logging.backtrace     | Configuration for Rust backtrace generation, set in the environment variable RUST_BACKTRACE.                         |               | true                        |
| sleeper.logging.rust          | Configuration for Rust logging, set in the environment variable RUST_LOG.                                            |               | true                        |
