package sleeper.systemtest.drivers.util;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public interface ReceiveMessages {

    List<Message> receiveAndDeleteMessages(String queueUrl, int maxNumberOfMessages, int waitTimeSeconds);

    public static ReceiveMessages from(SqsClient sqsClient) {
        return (queueUrl, maxNumberOfMessages, waitTimeSeconds) -> sqsReceiveAndDeleteMessages(
                sqsClient, queueUrl, maxNumberOfMessages, waitTimeSeconds);
    }

    private static List<Message> sqsReceiveAndDeleteMessages(SqsClient sqsClient, String queueUrl, int maxNumberOfMessages, int waitTimeSeconds) {
        List<Message> messages = sqsClient.receiveMessage(request -> request
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxNumberOfMessages)
                .waitTimeSeconds(waitTimeSeconds))
                .messages();
        if (messages.isEmpty()) {
            return List.of();
        }
        DeleteMessageBatchResponse deleteResult = sqsClient.deleteMessageBatch(request -> request
                .queueUrl(queueUrl)
                .entries(messages.stream()
                        .map(message -> DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .toList()));
        if (!deleteResult.failed().isEmpty()) {
            throw new RuntimeException("Failed deleting messages: " + deleteResult.failed());
        }
        return messages;
    }
}