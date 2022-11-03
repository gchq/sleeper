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
package sleeper.clients;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import sleeper.clients.admin.AdminConfigStore;
import sleeper.clients.admin.AdminMainScreen;
import sleeper.clients.admin.InstancePropertyReport;
import sleeper.clients.admin.TableNamesReport;
import sleeper.clients.admin.TablePropertyReport;
import sleeper.clients.admin.TablePropertyReportScreen;
import sleeper.clients.admin.UpdatePropertyScreen;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

import java.io.IOException;

public class AdminClient {
    private static final String CLEAR_CONSOLE = "\033[H\033[2J";
    private static final String INPUT_EMPTY = "\nYou did not enter anything please try again\n";
    private static final String PROPERTY_LOAD_ERROR =
            "There has been a problem loading or changing properties from/in the S3 bucket. \nThe error message was: ";
    private static final String TABLE_NULL_ERROR = "When a table property is being updated e.g. sleeper.table.* " +
            "then a Table Name must be provided in the parameters";
    private static final String PROPERTY_ERROR = "A property update error has occurred either the property name " +
            "or value is invalid. \nThe error message was: ";
    private static final String EXIT_PROGRAM_OPTION = "[0] Exit program";
    private static final String MAIN_MENU_OPTION = "[1] Return to Main Menu\n";

    private static AmazonS3 defaultS3Client;
    private final AdminConfigStore store;
    private final ConsoleOutput out;
    private final ConsoleInput in;

    public AdminClient(AdminConfigStore store, ConsoleOutput out, ConsoleInput in) {
        this.store = store;
        this.out = out;
        this.in = in;
    }

    public static AdminClient client(AmazonS3 s3Client) {
        return new AdminClient(
                new AdminConfigStore(s3Client),
                new ConsoleOutput(System.out),
                new ConsoleInput(System.console()));
    }

    static void printInstancePropertiesReport(AmazonS3 s3Client, String instanceId) {
        client(s3Client).instancePropertyReport().print(instanceId);
    }

    static void printTablePropertiesReport(AmazonS3 s3Client, String instanceId, String tableName) {
        client(s3Client).printTablePropertiesReport(instanceId, tableName);
    }

    private void printTablePropertiesReport(String instanceId, String tableName) {
        new TablePropertyReport(out, store).print(instanceId, tableName);
    }

    static void printTablesReport(AmazonS3 s3Client, String instanceId) {
        client(s3Client).tableNamesReport().print(instanceId);
    }

    static void updateProperty(AmazonS3 s3Client, String instanceId, String propertyName, String propertyValue, String tableName)
            throws IOException, AmazonS3Exception {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

        if (propertyName.startsWith("sleeper.table.") && tableName == null) {
            throw new IllegalArgumentException(TABLE_NULL_ERROR);
        }
        if (tableName == null) {
            client(s3Client).store.updateInstanceProperty(instanceId, propertyName, propertyValue);
        } else {
            client(s3Client).store.updateTableProperty(instanceId, tableName, propertyName, propertyValue);
        }
        System.out.println(propertyName + " has been updated to " + propertyValue);
    }

    public static void main(String[] args) {
        defaultS3Client = AmazonS3ClientBuilder.defaultClient();
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }
        client(defaultS3Client).start(args[0]);
    }

    public void start(String instanceId) {
        new AdminMainScreen(out, in).mainLoop(this, instanceId);
    }

    public InstancePropertyReport instancePropertyReport() {
        return new InstancePropertyReport(out, store);
    }

    public TableNamesReport tableNamesReport() {
        return new TableNamesReport(out, store);
    }

    public TablePropertyReportScreen tablePropertyReportScreen() {
        return new TablePropertyReportScreen(out, in, store);
    }

    public UpdatePropertyScreen updatePropertyScreen() {
        return new UpdatePropertyScreen(out, in, store);
    }
}
