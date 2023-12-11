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

import java.util.List;

public class FilesReportTestHelper {

    private FilesReportTestHelper() {
    }

    public static AllFileReferences wholeFilesReport(FileInfo... files) {
        return new AllFileReferences(List.of(files), List.of());
    }

    public static AllFileReferences splitFileReport(FileInfo... references) {
        return new AllFileReferences(List.of(references), List.of());
    }

    public static AllFileReferences readyForGCFilesReport(String... filename) {
        return new AllFileReferences(List.of(), List.of(filename));
    }

    public static AllFileReferences partialReadyForGCFilesReport(String... filename) {
        return new AllFileReferences(List.of(), List.of(filename), true);
    }
}
