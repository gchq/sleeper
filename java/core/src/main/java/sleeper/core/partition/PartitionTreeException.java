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
package sleeper.core.partition;

/**
 * Custom exception of illegal events concerning partition trees.
 */
public class PartitionTreeException extends Exception {

    final PartitionTree originalTree;

    public PartitionTreeException(String message, PartitionTree originalTree, Throwable e) {
        super(message, e);
        this.originalTree = originalTree;
    }

    public PartitionTreeException(String message, PartitionTree originalTree) {
        super(message);
        this.originalTree = originalTree;
    }

    public PartitionTree getOriginalPartitionTree() {
        return originalTree;
    }

}
