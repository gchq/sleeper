Support for in-place upgrade
============================

For all future versions of Sleeper, we intend to support upgrading a Sleeper instance in place without significant
disruption to the running instance. This is provided the instance is deployed with our standard CDK deployment code.

Our CDK deployment involves a large number of components, which communicate with each other in various formats, and is
backed by various layers of persistence. We also provide client code to interact with an instance of Sleeper.

Here are some changes we might want to make to Sleeper that could cause problems:

- Changes to communication or persistence formats, e.g. DynamoDB tables, SQS messages, JSON files, properties files, Arrow files
- Changes to how components join together, e.g. restructuring SQS queues, tracking mechanisms, temporary files in S3
- Changes to CDK deployment that may trigger CloudFormation to recreate resources (e.g. renaming resources, modifying an EMR cluster)
- Changes to how clients communicate with Sleeper

We can look at some checks you can do before a release to make it less likely problems will occur.

We have a separate document for checks on [changes to data formats during upgrade](upgrade-format-changes.md).

### Joining components together

Changes to how components communicate can easily cause problems. For example, if we wish to replace an SQS queue with
a different mechanism, we need to ensure that existing messages on the queue will be processed. If we simply replace
the queue in a CDK stack, any existing messages would likely be lost. One potential approach would be to leave the old
infrastructure in place and add the new mechanism alongside it. This could easily become difficult to manage.

This also applies to cases where we save a file to S3, and then read it from another process. If we ever stop writing
the file, or change when we delete it, that can easily cause similar problems.

We'd need to think very carefully about the potential use cases and options for migration, and how we can guarantee that
the CDK will do the right thing to ensure existing data is processed correctly.

By default we can make sure we don't make changes to existing infrastructure.

### Changes to CDK deployment triggering resource recreation

There are a number of changes we could make that would cause CloudFormation to delete and recreate resources in AWS.
For certain resources this can have a big impact on a Sleeper instance. We'll look at a couple of examples.

If we rename an S3 bucket this would delete the old bucket, which could result in data loss.

If we applied a change that caused CloudFormation to recreate a persistent EMR cluster, that would result in any
in-progress bulk import jobs being aborted. That could be a significant disruption, and potentially result in data
loss.

By default we can check that we don't change any resource names or construct IDs.

Ideally we could test an upgrade with all optional components included, and have some automated process to warn us when
resources are recreated as part of an upgrade. We don't have that right now, and it could be difficult to find such a
problem.
