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
package sleeper.systemtest.dsl.compaction;

import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.util.SplitIntoBatches;

import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StreamFakeCompactions {

    private final int numCompactions;
    private final IntFunction<List<FileReference>> generateInputFiles;
    private final IntFunction<String> generateJobId;
    private final IntFunction<FileReference> generateOutputFile;

    private StreamFakeCompactions(Builder builder) {
        numCompactions = builder.numCompactions;
        generateInputFiles = builder.generateInputFiles;
        generateJobId = builder.generateJobId;
        generateOutputFile = builder.generateOutputFile;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Stream<AddFilesTransaction> streamAddFiles() {
        return SplitIntoBatches.streamBatchesOf(10000, streamFilesToAdd())
                .map(AddFilesTransaction::fromReferences);
    }

    public Stream<AssignJobIdsTransaction> streamAssignJobIds() {
        return SplitIntoBatches.streamBatchesOf(10000, streamAssignJobIdRequests())
                .map(AssignJobIdsTransaction::new);
    }

    public Stream<CompactionCommitMessage> streamCommitMessages(String tableId) {
        return streamCommitRequests().map(request -> new CompactionCommitMessage(tableId, request));
    }

    public List<String> listJobIds() {
        return IntStream.rangeClosed(1, numCompactions)
                .mapToObj(generateJobId).toList();
    }

    public Stream<FileReference> streamOutputFiles() {
        return IntStream.rangeClosed(1, numCompactions)
                .mapToObj(generateOutputFile);
    }

    public Stream<ReplaceFileReferencesRequest> streamCommitRequests() {
        return IntStream.rangeClosed(1, numCompactions)
                .mapToObj(i -> generateCommitRequest(i));
    }

    private Stream<AssignJobIdRequest> streamAssignJobIdRequests() {
        return IntStream.rangeClosed(1, numCompactions)
                .mapToObj(this::generateAssignIdRequest);
    }

    private Stream<FileReference> streamFilesToAdd() {
        return IntStream.rangeClosed(1, numCompactions)
                .mapToObj(generateInputFiles)
                .flatMap(List::stream);
    }

    private AssignJobIdRequest generateAssignIdRequest(int index) {
        String jobId = generateJobId.apply(index);
        List<FileReference> inputFiles = generateInputFiles.apply(index);
        return AssignJobIdRequest.assignJobOnPartitionToFiles(jobId,
                partitionId(inputFiles), inputFilenames(inputFiles));
    }

    private ReplaceFileReferencesRequest generateCommitRequest(int index) {
        String jobId = generateJobId.apply(index);
        List<FileReference> inputFiles = generateInputFiles.apply(index);
        FileReference outputFile = generateOutputFile.apply(index);
        return ReplaceFileReferencesRequest.builder()
                .jobId(jobId)
                .taskId("fake-task")
                .jobRunId(jobId + "-run")
                .inputFiles(inputFilenames(inputFiles))
                .newReference(outputFile)
                .build();
    }

    private static String partitionId(List<FileReference> inputFiles) {
        return inputFiles.get(0).getPartitionId();
    }

    private static List<String> inputFilenames(List<FileReference> inputFiles) {
        return inputFiles.stream().map(FileReference::getFilename).toList();
    }

    public static class Builder {
        private int numCompactions;
        private IntFunction<List<FileReference>> generateInputFiles;
        private IntFunction<String> generateJobId;
        private IntFunction<FileReference> generateOutputFile;

        private Builder() {
        }

        public Builder numCompactions(int numCompactions) {
            this.numCompactions = numCompactions;
            return this;
        }

        public Builder generateInputFiles(IntFunction<List<FileReference>> generateInputFiles) {
            this.generateInputFiles = generateInputFiles;
            return this;
        }

        public Builder generateJobId(IntFunction<String> generateJobId) {
            this.generateJobId = generateJobId;
            return this;
        }

        public Builder generateOutputFile(IntFunction<FileReference> generateOutputFile) {
            this.generateOutputFile = generateOutputFile;
            return this;
        }

        public StreamFakeCompactions build() {
            return new StreamFakeCompactions(this);
        }
    }

}
