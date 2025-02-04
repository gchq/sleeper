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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lambda that follows the transaction log of a Sleeper state store.
 */
public class TransactionLogTrackerLambda implements RequestHandler<KinesisEvent, Void> {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogTrackerLambda.class);

    @Override
    public Void handleRequest(KinesisEvent event, Context context) {
        LOGGER.debug("Received event: {}", event);
        return null;
    }

}
