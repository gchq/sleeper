/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest;

import sleeper.statestore.FileInfo;

import java.util.List;

public class IngestResult {
    private final List<FileInfo> fileInfoList;

    private IngestResult(List<FileInfo> fileInfoList) {
        this.fileInfoList = fileInfoList;
    }

    public static IngestResult from(List<FileInfo> fileInfoList) {
        return new IngestResult(fileInfoList);
    }

    public long getNumberOfRecords() {
        return fileInfoList.stream()
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
    }
}
