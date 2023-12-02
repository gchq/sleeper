/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.core.statestore;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilesReportTestHelper {

    private FilesReportTestHelper() {
    }

    public static FilesReport wholeFilesReport(FileInfo... files) {
        return new FilesReport(Stream.of(files)
                .map(file -> new FileReferences(file.getFilename(), Instant.ofEpochMilli(file.getLastStateStoreUpdateTime()), List.of(file)))
                .collect(Collectors.toUnmodifiableList()));
    }

    public static FilesReport readyForGCFileReport(String filename, Instant lastUpdateTime) {
        return new FilesReport(List.of(new FileReferences(filename, lastUpdateTime, List.of())));
    }

    public static FilesReport splitFileReport(String filename, Instant lastUpdateTime, FileInfo... references) {
        return new FilesReport(List.of(
                new FileReferences(filename, lastUpdateTime, List.of(references))));
    }
}
