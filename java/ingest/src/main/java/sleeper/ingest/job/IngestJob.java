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
package sleeper.ingest.job;

import java.util.List;
import java.util.Objects;

public class IngestJob {
    private final String tableName;
    private final String id;
    private final List<String> files;

    public IngestJob(String tableName, String id, List<String> files) {
        this.tableName = tableName;
        this.id = id;
        this.files = files;
    }

    public String getTableName() {
        return tableName;
    }

    public String getId() {
        return id;
    }

    public List<String> getFiles() {
        return files;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestJob ingestJob = (IngestJob) o;
        return Objects.equals(id, ingestJob.id) &&
                Objects.equals(tableName, ingestJob.tableName) &&
                Objects.equals(files, ingestJob.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tableName, files);
    }

    @Override
    public String toString() {
        return "IngestJob{" +
                "id='" + id + '\'' +
                ", tableName=" + tableName +
                ", files=" + files +
                '}';
    }
}
