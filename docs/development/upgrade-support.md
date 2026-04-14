Support for in-place upgrade
============================

For all future versions of Sleeper, we intend to support upgrading a Sleeper instance in place without significant
disruption to the running instance. This is provided the instance is deployed with our standard CDK deployment code.

Our CDK deployment involves a large number of components, which communicate with each other in various formats, and is
backed by various layers of persistence. We also provide client code to interact with an instance of Sleeper.

Here are some changes we might want to make to Sleeper that could cause problems:

- Changes to communication or persistence formats, e.g. DynamoDB tables, SQS messages, JSON files, properties files, Arrow files
- Changes to how components join together, e.g. restructuring SQS queues, tracking mechanisms, temporary files in S3
- Changes to how clients communicate with Sleeper
- Changes to names of deployed resources
- Changes to CDK deployment that may trigger CloudFormation to recreate resources (e.g. EMR cluster)

We can look at some checks you can do before a release to make it less likely problems will occur.

We have a separate document for checks on [changes to data formats during upgrade](upgrade-format-changes.md).

### Joining components together

Changes to how components communicate can easily cause problems. For example, if we wish to replace an SQS queue with
a different mechanism, we need to ensure that existing messages on the queue will be processed. If we simply replace
the queue in a CDK stack, any existing messages would likely be lost. One potential approach would be to leave the old
infrastructure in place and add the new mechanism alongside it. This could easily become difficult to manage.

We'd need to think very carefully about the potential use cases and options for migration, and how we can guarantee that
the CDK will do the right thing to ensure existing data is processed correctly.

By default we can make sure we don't make changes to existing infrastructure.

### Changes to names of deployed resources

If we want to change names of anything deployed via the CDK, or IDs of any CDK constructs, by default this will involve
deleting the old resources and creating new ones. This usually not desirable, and would result in massive disruption
and potential data loss.
