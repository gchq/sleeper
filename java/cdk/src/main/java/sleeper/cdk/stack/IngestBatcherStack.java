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
package sleeper.cdk.stack;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class IngestBatcherStack extends NestedStack {

    public IngestBatcherStack(
            Construct scope,
            String id,
            BuiltJars jars,
            IngestStack ingestStack,
            EmrBulkImportStack emrBulkImportStack,
            PersistentEmrBulkImportStack persistentEmrBulkImportStack,
            EksBulkImportStack eksBulkImportStack) {
        super(scope, id);
    }

    private static Stream<Queue> ingestQueues(IngestStack ingestStack,
                                              EmrBulkImportStack emrBulkImportStack,
                                              PersistentEmrBulkImportStack persistentEmrBulkImportStack,
                                              EksBulkImportStack eksBulkImportStack) {
        return Stream.of(
                        ingestQueue(ingestStack, IngestStack::getIngestJobQueue),
                        ingestQueue(emrBulkImportStack, EmrBulkImportStack::getBulkImportJobQueue),
                        ingestQueue(persistentEmrBulkImportStack, PersistentEmrBulkImportStack::getBulkImportJobQueue),
                        ingestQueue(eksBulkImportStack, EksBulkImportStack::getBulkImportJobQueue))
                .flatMap(Optional::stream);
    }

    private static <T> Optional<Queue> ingestQueue(T stack, Function<T, Queue> getter) {
        return Optional.ofNullable(stack).map(getter);
    }
}
