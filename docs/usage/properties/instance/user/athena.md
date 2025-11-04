## Instance Properties - Athena - User Defined

The following instance properties relate to the integration with Athena.

| Property Name                           | Description                                                                                                                                                                          | Default Value | Run CDK Deploy When Changed |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------|
| sleeper.athena.spill.bucket.ageoff.days | The number of days before objects in the spill bucket are deleted.                                                                                                                   |               | true                        |
| sleeper.athena.handler.classes          | The fully qualified composite classes to deploy. These are the classes that interact with Athena. You can choose to remove one if you don't need them. Both are deployed by default. |               | true                        |
| sleeper.athena.handler.memory.mb        | The amount of memory (MB) the athena composite handler has.                                                                                                                          |               | true                        |
| sleeper.athena.handler.timeout.seconds  | The timeout in seconds for the athena composite handler.                                                                                                                             |               | true                        |
| sleeper.athena.spill.master.key.arn     | ARN of the KMS Key used to encrypt data in the Athena spill bucket.                                                                                                                  |               | true                        |
