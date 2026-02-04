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

package sleeper.systemtest.dsl.sourcedata;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

public class SourceFilesDsl {

    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext context;
    private final IngestSourceFilesDriver driver;
    private SourceFilesFolder sourceFolder;
    private boolean writeSketches = false;

    public SourceFilesDsl(SystemTestContext context, IngestSourceFilesDriver driver) {
        this.instance = context.instance();
        this.context = context.sourceFiles();
        this.driver = driver;
        this.sourceFolder = SourceFilesFolder.writeToSystemTestBucket(context);
    }

    public SourceFilesDsl inDataBucket() {
        sourceFolder = SourceFilesFolder.writeToDataBucket(instance);
        return this;
    }

    public SourceFilesDsl writeSketches() {
        writeSketches = true;
        return this;
    }

    public SourceFilesDsl createWithNumberedRows(String filename, LongStream numbers) {
        return create(filename, instance.numberedRows().iteratorFrom(numbers));
    }

    public SourceFilesDsl createWithNumberedRows(Schema schema, String filename, LongStream numbers) {
        return create(schema, filename, instance.numberedRows(schema).iteratorFrom(numbers));
    }

    public SourceFilesDsl create(String filename, Row... rows) {
        return create(filename, List.of(rows));
    }

    public SourceFilesDsl create(String filename, Iterable<Row> rows) {
        return create(filename, rows.iterator());
    }

    private SourceFilesDsl create(String filename, Iterator<Row> rows) {
        context.writeFile(driver, filename, sourceFolder, writeSketches, rows);
        return this;
    }

    private SourceFilesDsl create(Schema schema, String filename, Iterator<Row> rows) {
        context.writeFile(driver, schema, filename, sourceFolder, writeSketches, rows);
        return this;
    }
}
