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
package sleeper.cdk.stack.query;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;

/**
 * Deploys an SQS queue for queries to be sent to. These will be processed by lambdas defined in the {@link QueryStack}.
 */
public class QueryQueueStack extends NestedStack {
    public static final String QUERY_QUEUE_NAME = "QueryQueueName";
    public static final String QUERY_QUEUE_URL = "QueryQueueUrl";
    public static final String QUERY_DLQ_URL = "QueryDLQUrl";
    private Queue queryQueue;

    public QueryQueueStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            SleeperCoreStacks coreStacks) {
        super(scope, id);
        queryQueue = setupQueryQueue(instanceProperties, coreStacks);
    }

    /***
     * Creates the queue used for queries.
     *
     * @param  instanceProperties containing configuration details
     * @param  coreStacks         a reference to the core stacks, used for grants
     *
     * @return                    the queue to be used for queries
     */
    private Queue setupQueryQueue(InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlQueueName = String.join("-", "sleeper", instanceId, "QueryDLQ");
        Queue queryDlq = Queue.Builder
                .create(this, "QueryDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue queryDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queryDlq)
                .build();
        String queryQueueName = String.join("-", "sleeper", instanceId, "QueryQueue");
        Queue queryQueue = Queue.Builder
                .create(this, "QueryQueue")
                .queueName(queryQueueName)
                .deadLetterQueue(queryDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        queryQueue.grantSendMessages(coreStacks.getQueryPolicyForGrants());
        queryQueue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_URL, queryQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_ARN, queryQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_URL, queryDlq.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_ARN, queryDlq.getQueueArn());
        coreStacks.alarmOnDeadLetters(this, "QueryAlarm", "queries", queryDlq);
        CfnOutputProps queryQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queryQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_QUEUE_NAME, queryQueueOutputNameProps);

        CfnOutputProps queryQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_QUEUE_URL, queryQueueOutputProps);

        CfnOutputProps querDlqOutputProps = new CfnOutputProps.Builder()
                .value(queryDlq.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_DLQ_URL)
                .build();
        new CfnOutput(this, QUERY_DLQ_URL, querDlqOutputProps);

        return queryQueue;
    }

    public void grantSendMessages(IGrantable grantable) {
        queryQueue.grantSendMessages(grantable);
    }

    public Queue getQueue() {
        return queryQueue;
    }
}
