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

package sleeper.systemtest.datageneration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.ingest.batcher.submitter.FileIngestRequestSerDe;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class IngestRandomDataViaBatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomDataViaBatcher.class);

    private IngestRandomDataViaBatcher() {
    }

    public static void sendRequest(String dir, InstanceIngestSession session) {
        String queueUrl = session.instanceProperties().get(INGEST_BATCHER_SUBMIT_QUEUE_URL);
        String jsonRequest = FileIngestRequestSerDe.toJson(List.of(dir), session.tableProperties().get(TABLE_NAME));
        LOGGER.debug("Sending message to ingest batcher queue {}: {}", queueUrl, jsonRequest);
        session.sqs().sendMessage(queueUrl, jsonRequest);
    }
}
