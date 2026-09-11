## Instance Properties - Logging - User Defined

The following instance properties relate to logging.

| Property Name                 | Description                                                                                                                           | Default Value | Run CDK Deploy When Changed |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------|
| sleeper.logging.level         | The logging level for Sleeper classes. This overrides the corresponding Log4j setting, except for MetricsLogger which is always INFO. | INFO          | true                        |
| sleeper.logging.apache.level  | The logging level for Apache libraries other than Parquet. This overrides the corresponding Log4j setting.                            | INFO          | true                        |
| sleeper.logging.parquet.level | The logging level for Apache Parquet. This overrides the corresponding Log4j setting.                                                 | WARN          | true                        |
| sleeper.logging.aws.level     | The logging level for AWS SDK libraries. This overrides the corresponding Log4j setting.                                              | INFO          | true                        |
| sleeper.logging.root.level    | The root logging level for messages not covered by a more specific category. This overrides the corresponding Log4j setting.          | INFO          | true                        |
| sleeper.logging.backtrace     | Configuration for Rust backtrace generation, set in the environment variable RUST_BACKTRACE.                                          |               | true                        |
| sleeper.logging.rust          | Configuration for Rust logging, set in the environment variable RUST_LOG.                                                             |               | true                        |
