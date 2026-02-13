/*
 * Copyright 2022-2025 Crown Copyright
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
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.util.Utils;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * Deploys the resources needed to create and execute compaction jobs. This is done by delegating
 * to {@link CompactionJobResources} which creates resources that create and commit compaction jobs
 * and to {@link CompactionTaskResources} which creates resources that run compaction tasks that
 * process compaction jobs.
 */
public class CompactionStack extends NestedStack {
    public static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    public static final String COMPACTION_STACK_DLQ_URL = "CompactionStackDLQUrlKey";
    public static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";

    private CompactionJobResources jobResources;

    public CompactionStack(
            Construct scope, String id,
            SleeperInstanceProps props,
            SleeperCoreStacks coreStacks) {
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

        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", props.getInstanceProperties().get(JARS_BUCKET));
        SleeperLambdaCode lambdaCode = props.getArtefacts().lambdaCode(this);

        jobResources = new CompactionJobResources(this, props, lambdaCode, jarsBucket, coreStacks);

        new CompactionTaskResources(this, props, lambdaCode, jarsBucket, jobResources, coreStacks);

        Utils.addTags(this, props.getInstanceProperties());
    }

    public Queue getCompactionJobsQueue() {
        return jobResources.getCompactionJobsQueue();
    }
}
