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

package sleeper.statestore.s3;

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

import sleeper.core.statestore.StateStoreException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

class InMemoryRevisionStore implements RevisionStore {

    private final Map<String, S3RevisionId> revisionByKey = new HashMap<>();
    private final Map<String, Iterator<Consumer<S3RevisionId>>> revisionUpdatesAfterQuery = new HashMap<>();

    @Override
    public S3RevisionId getCurrentRevisionId(String revisionIdKey) {
        S3RevisionId revisionId = currentRevisionId(revisionIdKey);
        Iterator<Consumer<S3RevisionId>> revisionUpdates = revisionUpdatesAfterQuery.getOrDefault(revisionIdKey, Collections.emptyIterator());
        if (revisionUpdates.hasNext()) {
            S3RevisionId nextRevisionId = revisionId.getNextRevisionId();
            revisionUpdates.next().accept(nextRevisionId);
            revisionByKey.put(revisionIdKey, nextRevisionId);
        }
        return revisionId;
    }

    @Override
    public void conditionalUpdateOfRevisionId(String revisionIdKey, S3RevisionId currentRevisionId, S3RevisionId newRevisionId) {
        S3RevisionId current = currentRevisionId(revisionIdKey);
        if (!Objects.equals(current, currentRevisionId)) {
            throw new ConditionalCheckFailedException("Current revision not equal");
        }
        revisionByKey.put(revisionIdKey, newRevisionId);
    }

    public void initialise(String revisionIdKey, S3RevisionId revisionId) {
        revisionByKey.put(revisionIdKey, revisionId);
    }

    private S3RevisionId currentRevisionId(String revisionIdKey) {
        return Optional.ofNullable(revisionByKey.get(revisionIdKey)).orElseThrow();
    }

    public <T> void setDataInContentionAfterQueries(S3FileStoreType<T> fileType, List<T> data) {
        revisionUpdatesAfterQuery.put(fileType.getRevisionIdKey(),
                data.stream().map(dataItem -> setData(fileType, dataItem)).iterator());
    }

    private <T> Consumer<S3RevisionId> setData(S3FileStoreType<T> fileType, T data) {
        return revisionId -> {
            try {
                fileType.writeData(data, fileType.getPath(revisionId));
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
