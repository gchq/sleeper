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
package sleeper.statestore.lambda.committer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

import sleeper.compaction.core.job.commit.CompactionCommitRequest;

import java.util.List;

/**
 * A lambda that combines multiple compaction commits into a single transaction per Sleeper table.
 */
public class CompactionCommitBatcherLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        List<CompactionCommitRequest> requests = event.getRecords().stream().map(this::readMessage).toList();
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleRequest'");
    }

    private CompactionCommitRequest readMessage(SQSMessage message) {
        return new CompactionCommitRequest(null, null, null);
    }

}
