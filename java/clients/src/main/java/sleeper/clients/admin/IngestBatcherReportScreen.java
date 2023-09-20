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

package sleeper.clients.admin;

import sleeper.clients.status.report.IngestBatcherReport;
import sleeper.clients.status.report.ingest.batcher.BatcherQuery;
import sleeper.clients.status.report.ingest.batcher.StandardIngestBatcherReporter;
import sleeper.clients.util.console.ConsoleHelper;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.menu.MenuOption;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadInstanceProperties;

public class IngestBatcherReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminClientPropertiesStore store;
    private final AdminClientStatusStoreFactory statusStores;

    public IngestBatcherReportScreen(ConsoleOutput out, ConsoleInput in,
                                     AdminClientPropertiesStore store, AdminClientStatusStoreFactory statusStores) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.statusStores = statusStores;
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> propertiesOpt = tryLoadInstanceProperties(out, in, store, instanceId);
        if (propertiesOpt.isPresent()) {
            InstanceProperties properties = propertiesOpt.get();
            out.clearScreen("");
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All files", () ->
                            runBatcherReport(properties, BatcherQuery.Type.ALL)),
                    new MenuOption("Pending files", () ->
                            runBatcherReport(properties, BatcherQuery.Type.PENDING))
            ).run();
        }
    }

    private void runBatcherReport(InstanceProperties properties, BatcherQuery.Type queryType) {
        new IngestBatcherReport(statusStores.loadIngestBatcherStatusStore(properties,
                store.createTablePropertiesProvider(properties)),
                new StandardIngestBatcherReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}
