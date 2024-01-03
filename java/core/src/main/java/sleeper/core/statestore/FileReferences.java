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
import java.util.Objects;

public class FileReferences {

    private final String filename;
    private final Instant lastUpdateTime;
    private final List<FileInfo> references;

    public FileReferences(String filename, Instant lastUpdateTime, List<FileInfo> references) {
        this.filename = filename;
        this.lastUpdateTime = lastUpdateTime;
        this.references = references;
    }

    public String getFilename() {
        return filename;
    }

    public List<FileInfo> getReferences() {
        return references;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileReferences that = (FileReferences) o;
        return Objects.equals(filename, that.filename) && Objects.equals(lastUpdateTime, that.lastUpdateTime) && Objects.equals(references, that.references);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, lastUpdateTime, references);
    }

    @Override
    public String toString() {
        return "FileReferences{" +
                "filename='" + filename + '\'' +
                ", lastUpdateTime=" + lastUpdateTime +
                ", references=" + references +
                '}';
    }
}
