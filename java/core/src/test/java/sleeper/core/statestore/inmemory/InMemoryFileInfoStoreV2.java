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

package sleeper.core.statestore.inmemory;

import sleeper.core.statestore.AddFilesRequest;
import sleeper.core.statestore.FileInfoStoreV2;
import sleeper.core.statestore.FileInfoV2;

import java.util.ArrayList;
import java.util.List;

public class InMemoryFileInfoStoreV2 implements FileInfoStoreV2 {

    private final List<FileInfoV2> files = new ArrayList<>();

    @Override
    public void finishIngest(AddFilesRequest request) {
        request.buildFileInfos().forEach(files::add);
    }

    @Override
    public List<FileInfoV2> getPartitionFiles() {
        return files;
    }
}
