Support for in-place upgrade
============================

For all future versions of Sleeper, we intend to support upgrading a Sleeper instance in place. This is provided the
instance is deployed with our standard CDK deployment code.

We will operate on an assumption of applications requiring constant uptime, and frequent upgrades to Sleeper to minimise
risk. Any upgrade to Sleeper should not require any downtime, and should apply to a running instance without significant
disruption.

Our CDK deployment involves a large number of components, which communicate with each other in various formats, and is
backed by various layers of persistence. We also provide client code to interact with an instance of Sleeper.

Here are some changes we might want to make to Sleeper that could cause problems:

- Changes to communication or persistence formats, e.g. DynamoDB tables, SQS messages, JSON files, properties files, Arrow files
- Changes to how components join together, e.g. restructuring SQS queues, tracking mechanisms, temporary files in S3
- Changes to CDK deployment that may trigger CloudFormation to recreate resources (e.g. renaming resources, modifying an EMR cluster)
- Changes to how clients communicate with Sleeper

We will look at some checks we can do before a release to make it less likely problems will occur.

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
loss. We are planning to improve the resilience of our use of EMR for bulk import to ensure jobs will still be
processed. We are also planning to introduce a way to run persistent EMR clusters that would avoid them being recreated
unintentionally by the CDK. Until then there is a significant risk of unintentionally shutting down a running cluster.

By default we can check that we don't change any resource names or construct IDs.

Ideally we could test an upgrade with all optional components included, and have some automated process to warn us when
resources are recreated as part of an upgrade. We don't have that right now, and it could be difficult to find such a
problem.

We have an issue to system test this:

https://github.com/gchq/sleeper/issues/7003

### Changes to how clients communicate with Sleeper

There are a number of ways that changes to Sleeper could affect support for clients that interact with an instance.
When we upgrade an instance, any old versions of clients need to continue to interact with both the old and new version
as the upgrade proceeds.

If we didn't support old client code, you'd need downtime during an upgrade, to upgrade all applications that use client
code to interact with Sleeper. It's important to avoid that to allow for frequent updates with minimal disruption.

This would also impact any users interacting with Sleeper directly. While we can expect users to try to keep their
client code up to date, that might not happen in practice until they notice a problem.

Whichever data formats the clients interact with, we need to maintain support for those interactions as they were in an
old version.

Whichever points of interaction the clients have with the infrastructure of Sleeper, we need to keep those
infrastructure elements in place for old versions of the clients to interact with. This is true even if we would prefer
to replace them.

It may be necessary at some point to drop support for clients on older versions as we change the points of interaction
with an instance of Sleeper. Right now the clients have a lot of interactions directly with the underlying data
persistence, rather than protecting the interaction points in any way against future change. If we ever need to do that,
we will need to be very clear about the process. Before we change the underlying infrastructure we will want to have
confidence that all deployed instances in practice are on a version that has protections against the change we want to
make.

### Making breaking changes non-breaking

It may be necessary in the future to make changes that currently would need to be backwards incompatible. We have a
couple of options for how to do that:

1. Introduce a mechanism to support the change we want to make, then wait for that to be deployed everywhere before we
   make the change
2. Introduce a backwards incompatible change with a plan for migration, and rely on existing deployments applying that
   migration

We will try to ensure we never introduce changes that would be backwards incompatible for any known deployed instance.
We will try to avoid ever using option 2 in a way where any manual steps are required for migration.

Any necessary migration should be applied automatically, either by a CDK deployment, or in slower time after a
deployment with continued support for unmigrated data.

In general we will try to avoid any need for migration. The bulk of data held in Sleeper will make some types of
migration impractical to begin with.
