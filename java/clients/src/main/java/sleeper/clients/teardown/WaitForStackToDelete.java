/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.clients.teardown;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.cloudformation.model.Stack;
import software.amazon.awssdk.services.cloudformation.model.StackStatus;

import sleeper.core.util.PollWithRetries;

public class WaitForStackToDelete {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForStackToDelete.class);
    private static final long STACK_DELETED_POLL_INTERVAL_MILLIS = 30000;
    private static final int STACK_DELETED_TIMEOUT_MILLIS = 30 * 60 * 1000;
    private final PollWithRetries poll;
    private final CloudFormationClient cloudFormationClient;
    private final String stackName;

    public WaitForStackToDelete(PollWithRetries poll, CloudFormationClient cloudFormationClient, String stackName) {
        this.poll = poll;
        this.cloudFormationClient = cloudFormationClient;
        this.stackName = stackName;
    }

    public static WaitForStackToDelete from(CloudFormationClient cloudFormationClient, String stackName) {
        return from(
                PollWithRetries.intervalAndPollingTimeout(STACK_DELETED_POLL_INTERVAL_MILLIS, STACK_DELETED_TIMEOUT_MILLIS),
                cloudFormationClient, stackName);
    }

    public static WaitForStackToDelete from(PollWithRetries poll, CloudFormationClient cloudFormationClient, String stackName) {
        return new WaitForStackToDelete(poll, cloudFormationClient, stackName);
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("stack has deleted", this::hasStackDeleted);
    }

    private boolean hasStackDeleted() {
        try {
            Stack stack = cloudFormationClient.describeStacks(builder -> builder.stackName(stackName)).stacks()
                    .stream().findFirst().orElseThrow();
            if (StackStatus.DELETE_FAILED.equals(stack.stackStatus())) {
                throw new DeleteFailedException(stack.stackName());
            }
            LOGGER.info("Stack {} is currently in state {}", stackName, stack.stackStatus());
            return stack.stackStatus().equals(StackStatus.DELETE_COMPLETE);
        } catch (CloudFormationException e) {
            LOGGER.info("Exception checking status of stack {}: {}", stackName, e.getMessage());
            return true;
        }
    }

    static class DeleteFailedException extends RuntimeException {
        DeleteFailedException(String stackName) {
            super("Failed to delete stack \"" + stackName + "\"");
        }
    }
}
