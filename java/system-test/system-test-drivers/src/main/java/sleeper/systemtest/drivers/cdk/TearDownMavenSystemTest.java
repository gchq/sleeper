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

package sleeper.systemtest.drivers.cdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.teardown.TearDownClients;
import sleeper.clients.teardown.TearDownInstance;
import sleeper.core.util.LoggedDuration;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.clients.util.ClientUtils.optionalArgument;

public class TearDownMavenSystemTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownMavenSystemTest.class);

    private TearDownMavenSystemTest() {
    }

    public static void tearDown(
            Path scriptsDir, List<String> shortIds, List<String> shortInstanceNames, List<String> standaloneInstanceIds,
            TearDownClients clients) throws IOException, InterruptedException {
        List<String> instanceIds = shortIds.stream()
                .flatMap(shortId -> shortInstanceNames.stream()
                        .map(shortInstanceName -> shortId + "-" + shortInstanceName))
                .collect(toUnmodifiableList());
        List<String> instanceIdsAndStandalone = Stream.concat(instanceIds.stream(), standaloneInstanceIds.stream())
                .collect(toUnmodifiableList());
        Instant startTime = Instant.now();
        LOGGER.info("Found system test short IDs to tear down: {}", shortIds);
        LOGGER.info("Found instance IDs to tear down: {}", instanceIdsAndStandalone);

        List<TearDownSystemTestDeployment> tearDownSystemTestDeployments = shortIds.stream()
                .map(deploymentId -> TearDownSystemTestDeployment.fromDeploymentId(clients, deploymentId))
                .collect(toUnmodifiableList());
        List<TearDownInstance> tearDownMavenInstances = instanceIds.stream()
                .map(instanceId -> TearDownInstance.builder().instanceId(instanceId).clients(clients).scriptsDir(scriptsDir).build())
                .collect(toUnmodifiableList());
        List<TearDownInstance> tearDownStandaloneInstances = instanceIds.stream()
                .map(instanceId -> TearDownTestInstance.builder().instanceId(instanceId).clients(clients).scriptsDir(scriptsDir).build())
                .collect(toUnmodifiableList());
        List<TearDownInstance> tearDownAllInstances = Stream.concat(tearDownMavenInstances.stream(), tearDownStandaloneInstances.stream()).collect(toUnmodifiableList());

        for (TearDownInstance instance : tearDownAllInstances) {
            instance.shutdownSystemProcesses();
            instance.deleteStack();
        }
        for (TearDownInstance instance : tearDownMavenInstances) {
            instance.waitForStackToDelete();
        }
        for (TearDownSystemTestDeployment deployment : tearDownSystemTestDeployments) {
            deployment.shutdownSystemProcesses();
            deployment.deleteStack();
        }
        for (TearDownInstance instance : tearDownStandaloneInstances) {
            instance.waitForStackToDelete();
            instance.cleanupAfterStackDeleted();
        }
        for (TearDownSystemTestDeployment deployment : tearDownSystemTestDeployments) {
            deployment.waitForStackToDelete();
            deployment.cleanupAfterAllInstancesAndStackDeleted();
        }

        LOGGER.info("Tear down finished, took {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Usage: <scripts-directory> <comma-separated-short-ids> <optional-comma-separated-short-instance-names> <optional-comma-separated-standalone-instance-ids>");
        }
        Path scriptsDir = Path.of(args[0]);
        List<String> shortIds = List.of(args[1].split(","));
        List<String> shortInstanceNames = optionalArgument(args, 2)
                .map(names -> List.of(names.split(",")))
                .orElse(List.of());
        List<String> standaloneInstanceIds = optionalArgument(args, 3)
                .map(names -> List.of(names.split(",")))
                .orElse(List.of());
        TearDownClients.withDefaults(clients -> tearDown(scriptsDir, shortIds, shortInstanceNames, standaloneInstanceIds, clients));
    }
}
