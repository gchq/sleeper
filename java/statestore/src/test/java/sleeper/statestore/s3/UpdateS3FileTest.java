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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleSupplier;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdateS3FileTest {

    private final InMemoryRevisionStore revisionStore = new InMemoryRevisionStore();
    private final InMemoryRevisionTrackedFileStore<Object> fileStore = new InMemoryRevisionTrackedFileStore<>();
    private final List<Long> foundWaits = new ArrayList<>();

    @Test
    void shouldUpdateOnFirstAttempt() throws Exception {
        updateWithAttempts(1, existing -> "new", existing -> "");
        assertThat(loadCurrentData()).isEqualTo("new");
        assertThat(foundWaits).isEmpty();
    }

    private void updateWithAttempts(int attempts, Function<Object, Object> update, Function<Object, String> condition)
            throws Exception {
        UpdateS3File.updateWithAttempts(noJitter(), waiter(), revisionStore,
                RevisionTrackedS3FileType.builder()
                        .description("object")
                        .revisionIdKey("objects")
                        .buildPathFromRevisionId(revisionId -> "files/" + revisionId.getRevision())
                        .store(fileStore)
                        .build(),
                attempts, update, condition);
    }

    private Object loadCurrentData() throws Exception {
        S3RevisionId revisionId = revisionStore.getCurrentRevisionId("objects");
        return fileStore.load("files/" + revisionId.getRevision());
    }

    private static DoubleSupplier noJitter() {
        return () -> 0.0;
    }

    private UpdateS3File.Waiter waiter() {
        return foundWaits::add;
    }
}
