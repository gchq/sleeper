/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.admin.testutils;

import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.AdminClient;
import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableStatus;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;

public abstract class AdminClientInMemoryTestBase extends AdminClientTestBase {

    protected final List<CommandPipeline> commandsThatRan = new ArrayList<>();
    protected final CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
    protected final Map<Path, String> files = new HashMap<>();
    protected final Path scriptsDirectory = Path.of("./test");
    protected final DockerImageConfiguration dockerImageConfiguration = new DockerImageConfiguration(List.of(), List.of());
    protected final InMemoryAdminClientProperties clientProperties = InMemoryAdminClientProperties.createReturningExactInstance();
    protected final AdminClientPropertiesStore store = new AdminClientPropertiesStore(
            clientProperties, invokeCdk(), scriptsDirectory.resolve("generated"), uploadDockerImages(),
            dockerImageConfiguration);

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        super.setInstanceProperties(instanceProperties);
        clientProperties.setInstanceProperties(instanceProperties);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        clientProperties.createTablePropertiesStore(instanceProperties)
                .save(tableProperties);
    }

    @Override
    public void startClient(AdminClientTrackerFactory trackers, QueueMessageCount.Client queueClient) throws InterruptedException {
        new AdminClient(clientProperties.createTableIndex(instanceProperties), store, trackers,
                editor, out.consoleOut(), in.consoleIn(),
                queueClient, properties -> Collections.emptyMap())
                .start(instanceId);
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, TableStatus... tables) {
        setInstanceTables(instanceProperties, Stream.of(tables));
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, Stream<TableStatus> tables) {
        setInstanceProperties(instanceProperties);
        tables.map(table -> createValidTableProperties(instanceProperties, table))
                .forEach(this::saveTableProperties);
    }

    protected void setTableProperties(String tableName) {
        InstanceProperties properties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(properties, tableName);
        setInstanceProperties(properties, tableProperties);
    }

    protected void setTableProperties(TableStatus tableStatus) {
        InstanceProperties properties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(properties, tableStatus);
        setInstanceProperties(properties, tableProperties);
    }

    protected void setStateStoreForTable(String tableName, StateStore stateStore) {
        TableProperties tableProperties = clientProperties.createTablePropertiesStore(instanceProperties).loadByName(tableName);
        clientProperties.setStateStore(tableProperties, stateStore);
    }

    protected void verifyWithNumberOfPromptsBeforeExit(int numberOfInvocations) {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(numberOfInvocations)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    private InvokeCdk invokeCdk() {
        return InvokeCdk.builder()
                .version(version)
                .scriptsDirectory(scriptsDirectory)
                .runCommand(commandRunner)
                .build();
    }

    private UploadDockerImagesToEcr uploadDockerImages() {
        return new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .deployConfig(DeployConfiguration.fromLocalBuild())
                        .commandRunner(commandRunner)
                        .copyFile((source, target) -> files.put(target, files.get(source)))
                        .scriptsDirectory(scriptsDirectory)
                        .version(version)
                        .build(),
                account, region);
    }
}
