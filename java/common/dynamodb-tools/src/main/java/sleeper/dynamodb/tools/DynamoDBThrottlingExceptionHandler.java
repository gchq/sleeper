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
package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;

public class DynamoDBThrottlingExceptionHandler {
    private final ExceptionHandler throttlingExceptionHandler;
    private final ExceptionHandler otherExceptionHandler;

    public DynamoDBThrottlingExceptionHandler(ExceptionHandler throttlingExceptionHandler, ExceptionHandler otherExceptionHandler) {
        this.throttlingExceptionHandler = throttlingExceptionHandler;
        this.otherExceptionHandler = otherExceptionHandler;
    }

    public void handle(Exception e) {
        if (isThrottlingException(e)) {
            throttlingExceptionHandler.handle(e);
        } else {
            otherExceptionHandler.handle(e);
        }
    }

    private static boolean isThrottlingException(Exception e) {
        return e instanceof AmazonDynamoDBException
                && "ThrottlingException".equals(((AmazonDynamoDBException) e).getErrorCode());
    }

    @FunctionalInterface
    interface ExceptionHandler {
        void handle(Exception e);
    }
}
