/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.configuration.utils;

import java.util.List;
import java.util.stream.Stream;

/**
 * Results of expanding directories in S3.
 *
 * @param paths the paths that were expanded and their contents
 */
public record S3ExpandDirectoriesResult(List<S3PathContents> paths) {

    /**
     * Returns the files that were found, as Hadoop paths. Throws an exception if any requested path is empty. This is
     * in the format s3a://bucket-name/object-key.
     *
     * @return                         the Hadoop paths
     * @throws S3FileNotFoundException if any requested path is empty
     */
    public List<String> listHadoopPathsThrowIfAnyPathIsEmpty() throws S3FileNotFoundException {
        return streamFilesThrowIfAnyPathIsEmpty()
                .map(S3FileDetails::pathForHadoop)
                .toList();
    }

    /**
     * Returns the files that were found, as paths for ingest jobs. Also suitable for bulk import jobs or the ingest
     * batcher. This is in the format bucket-name/object-key.
     *
     * @return the paths
     */
    public List<String> listJobPaths() {
        return streamFiles()
                .map(S3FileDetails::pathForJob)
                .toList();
    }

    /**
     * Returns the requested paths that were not found. These are returned exactly as found in the request.
     *
     * @return the paths
     */
    public List<String> listMissingPaths() {
        return paths.stream()
                .filter(S3PathContents::isEmpty)
                .map(S3PathContents::requestedPath)
                .toList();
    }

    /**
     * Returns the files that were found. Throws an exception if any requested path is empty.
     *
     * @return                         the files
     * @throws S3FileNotFoundException if any requested path is empty
     */
    public Stream<S3FileDetails> streamFilesThrowIfAnyPathIsEmpty() throws S3FileNotFoundException {
        return paths.stream()
                .peek(S3PathContents::throwIfEmpty)
                .flatMap(path -> path.files().stream());
    }

    /**
     * Returns the files that were found.
     *
     * @return the files
     */
    public Stream<S3FileDetails> streamFiles() {
        return paths.stream()
                .flatMap(path -> path.files().stream());
    }
}
