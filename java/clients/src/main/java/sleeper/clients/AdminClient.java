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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.admin.AdminConfigStore;
import sleeper.clients.admin.AdminConfigStoreException;
import sleeper.clients.admin.AdminMainScreen;
import sleeper.clients.admin.FilesStatusReportScreen;
import sleeper.clients.admin.InstanceConfigurationScreen;
import sleeper.clients.admin.PartitionsStatusReportScreen;
import sleeper.clients.admin.TableNamesReport;
import sleeper.clients.admin.UpdatePropertiesWithNano;
import sleeper.clients.cdk.InvokeCdkForInstance;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AdminClient {

    private final AdminConfigStore store;
    private final UpdatePropertiesWithNano editor;
    private final ConsoleOutput out;
    private final ConsoleInput in;

    public AdminClient(AdminConfigStore store, UpdatePropertiesWithNano editor, ConsoleOutput out, ConsoleInput in) {
        this.store = store;
        this.editor = editor;
        this.out = out;
        this.in = in;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id>");
        }

        Path scriptsDir = Path.of(args[0]);
        String instanceId = args[1];
        Path generatedDir = scriptsDir.resolve("generated");
        Path jarsDir = scriptsDir.resolve("jars");
        String version = Files.readString(scriptsDir.resolve("templates/version.txt"));
        InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                .instancePropertiesFile(generatedDir.resolve("instance.properties"))
                .jarsDirectory(jarsDir).version(version).build();

        new AdminClient(
                new AdminConfigStore(
                        AmazonS3ClientBuilder.defaultClient(),
                        AmazonDynamoDBClientBuilder.defaultClient(),
                        cdk, generatedDir),
                new UpdatePropertiesWithNano(Path.of("/tmp")),
                new ConsoleOutput(System.out),
                new ConsoleInput(System.console())).start(instanceId);
    }

    public void start(String instanceId) throws InterruptedException {
        try {
            store.loadInstanceProperties(instanceId);
            new AdminMainScreen(out, in).mainLoop(this, instanceId);
        } catch (AdminConfigStoreException e) {
            out.println(e.getMessage());
            out.println("Cause: " + e.getCause().getMessage());
        }
    }

    public InstanceConfigurationScreen instanceConfigurationScreen() {
        return new InstanceConfigurationScreen(out, in, store, editor);
    }

    public TableNamesReport tableNamesReport() {
        return new TableNamesReport(out, in, store);
    }

    public PartitionsStatusReportScreen partitionsStatusReportScreen() {
        return new PartitionsStatusReportScreen(out, in, store);
    }

    public FilesStatusReportScreen filesStatusReportScreen() {
        return new FilesStatusReportScreen(out, in, store);
    }
}
