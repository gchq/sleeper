/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.cdk.stack.compaction;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

/**
 * Deploys the resources needed to perform compaction jobs. Specifically, there is:
 * <p>
 * - A lambda, that is periodically triggered by a CloudWatch rule, to query the state store for
 * information about active files with no job id, to create compaction job definitions as
 * appropriate and post them to a queue.
 * - An ECS {@link Cluster} and either a {@link FargateTaskDefinition} or a {@link Ec2TaskDefinition}
 * for tasks that will perform compaction jobs.
 * - A lambda, that is periodically triggered by a CloudWatch rule, to look at the
 * size of the queue and the number of running tasks and create more tasks if necessary.
 */
public class CompactionStack extends NestedStack {
    public static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    public static final String COMPACTION_STACK_DLQ_URL = "CompactionStackDLQUrlKey";
    public static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";

    private Queue compactionJobsQueue;

    public CompactionStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);
        // The compaction stack consists of the following components:
        // - An SQS queue for the compaction jobs.
        // - A lambda to periodically check for compaction jobs that should be created.
        //   This lambda is fired periodically by a CloudWatch rule. It queries the
        //   StateStore for information about the current partitions and files,
        //   identifies files that should be compacted, creates a job definition
        //   and sends it to an SQS queue.
        // - An ECS cluster, task definition, etc., for compaction jobs.
        // - A lambda that periodically checks the number of running compaction tasks
        //   and if there are not enough (i.e. there is a backlog on the queue
        //   then it creates more tasks).

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        // SQS queue for the compaction jobs
        compactionJobsQueue = new CompactionJobResources(this,
                instanceProperties, lambdaCode, jarsBucket, topic, coreStacks, errorMetrics)
                .getCompactionJobsQueue();

        new CompactionTaskResources(this,
                instanceProperties, lambdaCode, jarsBucket, compactionJobsQueue, topic, coreStacks, errorMetrics);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    public Queue getCompactionJobsQueue() {
        return compactionJobsQueue;
    }
}
