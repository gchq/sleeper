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
package sleeper.cdk.stack;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;

/**
 * A {@link NestedStack} consisting of a {@link Queue} that queries are sent to, to be processed by
 * lambdas defined in the {@link QueryStack}.
 */
public class QueryQueueStack extends NestedStack {
    public static final String QUERY_QUEUE_NAME = "QueryQueueName";
    public static final String QUERY_QUEUE_URL = "QueryQueueUrl";
    public static final String QUERY_DL_QUEUE_URL = "QueryDLQueueUrl";
    private Queue queryQueriesQueue;

    public QueryQueueStack(Construct scope,
            String id,
            InstanceProperties instanceProperties) {
        super(scope, id);
        queryQueriesQueue = setupQueriesQueryQueue(instanceProperties);
    }

    /***
     * Creates the queue used for queries.
     *
     * @param  instanceProperties containing configuration details
     * @return                    the queue to be used for queries
     */
    private Queue setupQueriesQueryQueue(InstanceProperties instanceProperties) {
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryDLQ");
        Queue queryQueueForDLs = Queue.Builder
                .create(this, "QueriesDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue queryDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queryQueueForDLs)
                .build();
        String queryQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueriesQueue");
        Queue queryQueue = Queue.Builder
                .create(this, "QueriesQueue")
                .queueName(queryQueueName)
                .deadLetterQueue(queryDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_URL, queryQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_ARN, queryQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_URL, queryQueueForDLs.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_ARN, queryQueueForDLs.getQueueArn());

        CfnOutputProps queriesQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queryQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_QUEUE_NAME, queriesQueueOutputNameProps);

        CfnOutputProps queriesQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_QUEUE_URL, queriesQueueOutputProps);

        CfnOutputProps queriesDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryQueueForDLs.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_DL_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_DL_QUEUE_URL, queriesDLQueueOutputProps);

        return queryQueue;
    }

    public void grantSendMessages(IGrantable grantable) {
        queryQueriesQueue.grantSendMessages(grantable);
    }

    public Queue getQueue() {
        return queryQueriesQueue;
    }
}
