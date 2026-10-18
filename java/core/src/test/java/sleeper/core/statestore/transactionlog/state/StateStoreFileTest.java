/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.statestore.transactionlog.state;

import org.junit.jupiter.api.Test;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StateStoreFileTest {
    private static final Instant UPDATE_TIME = Instant.parse("2026-10-18T10:00:00Z");

    @Test
    void shouldShareFilenameWhenLoadingReferences() {
        AllReferencesToAFile model = fileWithSeparateFilenames();

        StateStoreFile file = new StateStoreFile(model.getFilename(), UPDATE_TIME, model.getReferences());

        assertThat(file.toModel()).isEqualTo(model);
        assertThat(file.getReferences()).allSatisfy(reference ->
                assertThat(reference.getFilename()).isSameAs(file.getFilename()));
    }

    @Test
    void shouldShareFilenameWhenLoadingImmutableModel() {
        AllReferencesToAFile model = fileWithSeparateFilenames();

        StateStoreFile file = StateStoreFile.from(model);

        assertThat(file.toModel()).isEqualTo(model);
        assertThat(file.getReferences()).allSatisfy(reference ->
                assertThat(reference.getFilename()).isSameAs(file.getFilename()));
    }

    @Test
    void shouldShareFilenameWhenAddingFileWithMultipleReferences() {
        AllReferencesToAFile model = fileWithSeparateFilenames();
        Instant createdTime = UPDATE_TIME.plusSeconds(10);

        StateStoreFile file = StateStoreFile.newFile(createdTime, model);

        assertThat(file.toModel()).isEqualTo(model.withCreatedUpdateTime(createdTime));
        assertThat(file.getReferences()).allSatisfy(reference ->
                assertThat(reference.getFilename()).isSameAs(file.getFilename()));
    }

    @Test
    void shouldShareFilenameWhenAddingSingleReference() {
        FileReference reference = reference("root", 100);

        StateStoreFile file = StateStoreFile.newFile(UPDATE_TIME, reference);

        assertThat(file.getFilename()).isSameAs(reference.getFilename());
        assertThat(file.getReferences()).allSatisfy(actual -> {
            assertThat(actual).isEqualTo(reference.toBuilder().lastStateStoreUpdateTime(UPDATE_TIME).build());
            assertThat(actual.getFilename()).isSameAs(file.getFilename());
        });
    }

    @Test
    void shouldShareFilenameWhenSplittingAndUpdatingReferences() {
        StateStoreFile file = StateStoreFile.newFile(UPDATE_TIME, reference("root", 100));
        FileReference left = reference("left", 40);
        FileReference right = reference("right", 60);
        Instant splitTime = UPDATE_TIME.plusSeconds(1);
        Instant assignedTime = UPDATE_TIME.plusSeconds(2);
        Instant removedTime = UPDATE_TIME.plusSeconds(3);

        file.splitReferenceFromPartition("root", List.of(left, right), splitTime);

        assertThat(file.toModel().getReferences()).containsExactly(
                left.toBuilder().lastStateStoreUpdateTime(splitTime).build(),
                right.toBuilder().lastStateStoreUpdateTime(splitTime).build());
        assertThat(file.getReferences()).allSatisfy(reference ->
                assertThat(reference.getFilename()).isSameAs(file.getFilename()));
        assertThat(file.getLastStateStoreUpdateTime()).isEqualTo(splitTime);

        file.setJobIdForPartition("job", "left", assignedTime);
        file.removeReferenceForPartition("right", removedTime);

        FileReference assigned = file.getReferenceForPartitionId("left").orElseThrow();
        assertThat(assigned).isEqualTo(left.toBuilder().jobId("job").lastStateStoreUpdateTime(assignedTime).build());
        assertThat(assigned.getFilename()).isSameAs(file.getFilename());
        assertThat(file.getReferences()).containsExactly(assigned);
        assertThat(file.getLastStateStoreUpdateTime()).isEqualTo(removedTime);
        assertThat(StateStoreFile.from(file.toModel()).getReferences()).allSatisfy(reference ->
                assertThat(reference.getFilename()).isSameAs(file.getFilename()));
    }

    @Test
    void shouldReuseReferencesAlreadySharingFilename() {
        FileReference reference = reference("root", 100);

        StateStoreFile file = new StateStoreFile(reference.getFilename(), UPDATE_TIME, List.of(reference));

        assertThat(file.getReferenceForPartitionId("root").orElseThrow()).isSameAs(reference);
    }

    @Test
    void shouldKeepUnreferencedFile() {
        String filename = new String("file.parquet");
        AllReferencesToAFile model = AllReferencesToAFile.builder()
                .filename(filename).lastStateStoreUpdateTime(UPDATE_TIME).references(List.of()).build();

        StateStoreFile file = StateStoreFile.from(model);

        assertThat(file.getFilename()).isSameAs(filename);
        assertThat(file.toModel()).isEqualTo(model);
    }

    private AllReferencesToAFile fileWithSeparateFilenames() {
        return AllReferencesToAFile.builder()
                .filename(new String("file.parquet"))
                .lastStateStoreUpdateTime(UPDATE_TIME)
                .references(List.of(reference("left", 40), reference("right", 60)))
                .build();
    }

    private FileReference reference(String partitionId, long rows) {
        return FileReference.builder()
                .filename(new String("file.parquet"))
                .partitionId(partitionId)
                .numberOfRows(rows)
                .jobId("existing-job")
                .lastStateStoreUpdateTime(UPDATE_TIME)
                .countApproximate(true)
                .onlyContainsDataForThisPartition(false)
                .build();
    }
}
