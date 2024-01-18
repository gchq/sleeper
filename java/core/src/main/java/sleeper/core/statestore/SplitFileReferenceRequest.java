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

package sleeper.core.statestore;

import java.util.List;

public class SplitFileReferenceRequest {
    private final FileReference oldReference;
    private final List<FileReference> newReferences;

    private SplitFileReferenceRequest(FileReference oldReference, List<FileReference> newReferences) {
        this.oldReference = oldReference;
        this.newReferences = newReferences;
    }

    public static SplitFileReferenceRequest splitFileToChildPartitions(FileReference file, String leftPartition, String rightPartition) {
        return new SplitFileReferenceRequest(file,
                List.of(SplitFileReference.referenceForChildPartition(file, leftPartition),
                        SplitFileReference.referenceForChildPartition(file, rightPartition)));
    }

    public FileReference getOldReference() {
        return oldReference;
    }

    public List<FileReference> getNewReferences() {
        return newReferences;
    }
}
