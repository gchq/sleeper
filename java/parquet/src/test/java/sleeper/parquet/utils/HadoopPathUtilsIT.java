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
package sleeper.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

class HadoopPathUtilsIT {

    @TempDir
    public java.nio.file.Path folder;

    @Nested
    @DisplayName("Find files by their paths")
    class FindFiles {

        @Test
        void shouldGetPathsForFilesInOneDir() throws Exception {
            // Given
            String localDir = createTempDirectory(folder, null).toString();
            Configuration conf = new Configuration();
            List<String> files = new ArrayList<>();

            files.add(createTestFile(localDir, "file-1.parquet"));
            files.add(createTestFile(localDir, "file-2.parquet"));

            // When
            List<Path> pathsForIngest = HadoopPathUtils.getPaths(files, conf, "");

            // Then
            assertThat(pathsForIngest)
                    .extracting(path -> path.toUri().getPath())
                    .containsExactlyInAnyOrder(localDir + "/file-1.parquet", localDir + "/file-2.parquet");
        }

        @Test
        void shouldIgnoreCrcFiles() throws Exception {
            // Given
            String localDir = createTempDirectory(folder, null).toString();
            Configuration conf = new Configuration();
            List<String> files = new ArrayList<>();

            files.add(createTestFile(localDir, "file-0.parquet"));
            files.add(createTestFile(localDir, "file-1.crc"));
            files.add(createTestFile(localDir, "file-2.csv"));

            // When
            List<Path> pathsForIngest = HadoopPathUtils.getPaths(files, conf, "");

            // Then
            assertThat(pathsForIngest)
                    .extracting(path -> path.toUri().getPath())
                    .containsExactlyInAnyOrder(localDir + "/file-0.parquet", localDir + "/file-2.csv");
        }

        @Test
        void shouldReadFileSizes() throws Exception {
            // Given
            String localDir = createTempDirectory(folder, null).toString();
            Configuration conf = new Configuration();
            List<String> files = new ArrayList<>();

            files.add(createTestFile(localDir, "file-1.parquet", 2L));
            files.add(createTestFile(localDir, "file-2.parquet", 4L));

            // When
            Stream<FileStatus> fileStatuses = HadoopPathUtils.streamFiles(files, conf, "");

            // Then
            assertThat(fileStatuses)
                    .extracting(status -> status.getPath().getName(), FileStatus::getLen)
                    .containsExactlyInAnyOrder(
                            tuple("file-1.parquet", 2L),
                            tuple("file-2.parquet", 4L));
        }
    }

    @Nested
    @DisplayName("Find files by paths of directories")
    class FollowDirectories {

        @Test
        void shouldGetPathsForFilesInMultipleDirectories() throws Exception {
            // Given
            String localDir = createTempDirectory(folder, null).toString();
            Configuration conf = new Configuration();
            List<String> files = new ArrayList<>();

            for (int dir = 0; dir < 3; dir++) {
                Files.createDirectory(Paths.get(localDir + "/dir-" + dir));
                for (int i = 0; i < 2; i++) {
                    String outputFile = localDir + "/dir-" + dir + "/file-" + i + ".parquet";
                    Files.createFile(Paths.get(outputFile));
                }
                //provide all the sub-directories
                files.add(localDir + "/dir-" + dir + "/");
            }

            // When
            List<Path> pathsForIngest = HadoopPathUtils.getPaths(files, conf, "");

            // Then
            assertThat(pathsForIngest)
                    .extracting(path -> path.toUri().getPath())
                    .containsExactlyInAnyOrder(
                            localDir + "/dir-0/file-0.parquet", localDir + "/dir-0/file-1.parquet",
                            localDir + "/dir-1/file-0.parquet", localDir + "/dir-1/file-1.parquet",
                            localDir + "/dir-2/file-0.parquet", localDir + "/dir-2/file-1.parquet");
        }

        @Test
        void shouldGetPathsForFilesInNestedDirectories() throws Exception {
            // Given
            String localDir = createTempDirectory(folder, null).toString();
            Configuration conf = new Configuration();
            List<String> files = new ArrayList<>();

            for (int dir = 0; dir < 2; dir++) {
                Files.createDirectory(Paths.get(localDir + "/dir-" + dir));
                for (int i = 0; i < 2; i++) {
                    String outputFile = localDir + "/dir-" + dir + "/file-" + i + ".parquet";
                    Files.createFile(Paths.get(outputFile));
                }
            }

            Files.createDirectory(Paths.get(localDir + "/dir-0" + "/dir-nested"));
            String outputFile = localDir + "/dir-0/dir-nested/file-0.parquet";
            Files.createFile(Paths.get(outputFile));

            //provide only the highest directory
            files.add(localDir);

            // When
            List<Path> pathsForIngest = HadoopPathUtils.getPaths(files, conf, "");

            // Then
            assertThat(pathsForIngest)
                    .extracting(path -> path.toUri().getPath())
                    .containsExactlyInAnyOrder(
                            localDir + "/dir-0/file-0.parquet", localDir + "/dir-0/file-1.parquet",
                            localDir + "/dir-0/dir-nested/file-0.parquet",
                            localDir + "/dir-1/file-0.parquet", localDir + "/dir-1/file-1.parquet");
        }

        @Test
        void shouldReadFileSizesUnderADirectory() throws Exception {
            // Given
            String localDir = createTempDirectory(folder, null).toString();
            Configuration conf = new Configuration();
            createTestFile(localDir, "file-1.parquet", 2L);
            createTestFile(localDir, "file-2.parquet", 4L);

            // When
            Stream<FileStatus> fileStatuses = HadoopPathUtils.streamFiles(List.of(localDir), conf, "");

            // Then
            assertThat(fileStatuses)
                    .extracting(status -> status.getPath().getName(), FileStatus::getLen)
                    .containsExactlyInAnyOrder(
                            tuple("file-1.parquet", 2L),
                            tuple("file-2.parquet", 4L));
        }
    }

    @Nested
    @DisplayName("Find no files when none are present")
    class FindNoFiles {

        @Test
        void shouldReturnEmptyListIfNoFiles() {
            // Given
            Configuration conf = new Configuration();

            // When
            List<Path> pathsForIngest = HadoopPathUtils.getPaths(new ArrayList<>(), conf, "");

            // Then
            assertThat(pathsForIngest).isEmpty();
        }

        @Test
        void shouldReturnEmptyListIfNull() {
            // Given
            Configuration conf = new Configuration();

            // When
            List<Path> pathsForIngest = HadoopPathUtils.getPaths(null, conf, "");

            // Then
            assertThat(pathsForIngest).isEmpty();
        }

        @Test
        void shouldReturnEmptyListIfNoFilesGettingFileStatus() {
            // Given
            Configuration conf = new Configuration();

            // When / Then
            assertThat(HadoopPathUtils.streamFiles(new ArrayList<>(), conf, ""))
                    .isEmpty();
        }

        @Test
        void shouldReturnEmptyListIfNullGettingFileStatus() {
            // Given
            Configuration conf = new Configuration();

            // When / Then
            assertThat(HadoopPathUtils.streamFiles(null, conf, ""))
                    .isEmpty();
        }

        @Test
        void shouldFailWhenFileNotFoundAtSpecifiedPath() {
            assertThatThrownBy(() -> HadoopPathUtils.getPaths(
                    List.of("not-a-file.parquet"), new Configuration(), ""))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(FileNotFoundException.class);
        }
    }

    private static String createTestFile(String localDir, String fileName) throws Exception {
        return createTestFile(localDir, fileName, 0);
    }

    private static String createTestFile(String localDir, String fileName, long size) throws Exception {
        String outputFile = localDir + "/" + fileName;
        Files.writeString(Paths.get(outputFile), "a".repeat((int) size));
        return outputFile;
    }
}
