/*
 * Copyright 2023 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.admin.AdminConfigStore;
import sleeper.clients.admin.AdminMainScreen;
import sleeper.clients.admin.InstancePropertyReport;
import sleeper.clients.admin.TableNamesReport;
import sleeper.clients.admin.TablePropertyReportScreen;
import sleeper.clients.admin.UpdatePropertyScreen;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

public class AdminClient {

    private final AdminConfigStore store;
    private final ConsoleOutput out;
    private final ConsoleInput in;

    public AdminClient(AdminConfigStore store, ConsoleOutput out, ConsoleInput in) {
        this.store = store;
        this.out = out;
        this.in = in;
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }
        client(AmazonS3ClientBuilder.defaultClient()).start(args[0]);
    }

    public static AdminClient client(AmazonS3 s3Client) {
        return new AdminClient(
                new AdminConfigStore(s3Client),
                new ConsoleOutput(System.out),
                new ConsoleInput(System.console()));
    }

    public void start(String instanceId) {
        new AdminMainScreen(out, in).mainLoop(this, instanceId);
    }

    public InstancePropertyReport instancePropertyReport() {
        return new InstancePropertyReport(out, in, store);
    }

    public TableNamesReport tableNamesReport() {
        return new TableNamesReport(out, in, store);
    }

    public TablePropertyReportScreen tablePropertyReportScreen() {
        return new TablePropertyReportScreen(out, in, store);
    }

    public UpdatePropertyScreen updatePropertyScreen() {
        return new UpdatePropertyScreen(out, in, store);
    }
}
