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
package sleeper.bulkimport.runner;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.partition.Partition;
import sleeper.core.properties.table.TableProperties;

import java.util.List;
import java.util.function.Consumer;

public record FakeBulkImportContext(
        TableProperties tableProperties, List<Partition> partitions, BulkImportJob job,
        Consumer<FakeBulkImportContext> createContext, Consumer<FakeBulkImportContext> closeContext) implements BulkImportContext<FakeBulkImportContext> {

    public static BulkImportJobDriver.ContextCreator<FakeBulkImportContext> creator(List<FakeBulkImportContext> trackCreatedContexts, List<FakeBulkImportContext> trackClosedContexts) {
        return (tableProperties, partitions, job) -> {
            FakeBulkImportContext context = new FakeBulkImportContext(tableProperties, partitions, job, trackCreatedContexts::add, trackClosedContexts::add);
            trackCreatedContexts.add(context);
            return context;
        };
    }

    @Override
    public FakeBulkImportContext withPartitions(List<Partition> partitions) {
        FakeBulkImportContext newContext = new FakeBulkImportContext(tableProperties, partitions, job, createContext, closeContext);
        createContext.accept(newContext);
        return newContext;
    }

    @Override
    public void close() {
        closeContext.accept(this);
    }

}
