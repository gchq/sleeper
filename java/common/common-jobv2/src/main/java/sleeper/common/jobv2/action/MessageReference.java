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
package sleeper.common.jobv2.action;

import com.amazonaws.services.sqs.AmazonSQS;

public class MessageReference {

    private final AmazonSQS sqsClient;
    private final String sqsJobQueueUrl;
    private final String jobDescription;
    private final String receiptHandle;

    public MessageReference(AmazonSQS sqsClient, String sqsJobQueueUrl, String jobDescription, String receiptHandle) {
        this.sqsClient = sqsClient;
        this.sqsJobQueueUrl = sqsJobQueueUrl;
        this.jobDescription = jobDescription;
        this.receiptHandle = receiptHandle;
    }

    public DeleteMessageAction deleteAction() {
        return new DeleteMessageAction(this);
    }

    public ChangeMessageVisibilityTimeoutAction changeVisibilityTimeoutAction(int messageVisibilityTimeout) {
        return new ChangeMessageVisibilityTimeoutAction(this, messageVisibilityTimeout);
    }

    public AmazonSQS getSqsClient() {
        return sqsClient;
    }

    public String getSqsJobQueueUrl() {
        return sqsJobQueueUrl;
    }

    public String getJobDescription() {
        return jobDescription;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }
}
