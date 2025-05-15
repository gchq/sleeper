## Instance Properties - Logging - User Defined

The following instance properties relate to logging.

| Property Name                 | Description                                                                                                          | Default Value | Run CdkDeploy When Changed |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------|---------------|----------------------------|
| sleeper.logging.level         | The logging level for logging Sleeper classes. This does not apply to the MetricsLogger which is always set to INFO. |               | true                       |
| sleeper.logging.apache.level  | The logging level for Apache logs that are not Parquet.                                                              |               | true                       |
| sleeper.logging.parquet.level | The logging level for Parquet logs.                                                                                  |               | true                       |
| sleeper.logging.aws.level     | The logging level for AWS logs.                                                                                      |               | true                       |
| sleeper.logging.root.level    | The logging level for everything else.                                                                               |               | true                       |
