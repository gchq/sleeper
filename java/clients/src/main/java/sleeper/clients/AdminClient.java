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
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.UserExitedException;

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

        boolean foundProperty = false;

        if (tableName != null) {
            TablePropertiesProvider tablePropertiesProvider =
                    new TablePropertiesProvider(s3Client, instanceProperties);
            TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);

            for (TableProperty tableProperty : TableProperty.values()) {
                if (tableProperty.getPropertyName().equals(propertyName)) {
                    foundProperty = true;
                    tableProperties.set(tableProperty, propertyValue);
                    if (!tableProperty.validationPredicate().test(tableProperties.get(tableProperty))) {
                        throw new IllegalArgumentException("Sleeper property: " +
                                tableProperty.getPropertyName() + " is invalid");
                    }
                }
            }
            if (!foundProperty) {
                throw new IllegalArgumentException("Sleeper property: " + propertyName +
                        " does not exist and cannot be updated");
            }
            tableProperties.saveToS3(s3Client);
        } else {
            for (UserDefinedInstanceProperty userDefinedInstanceProperty : UserDefinedInstanceProperty.values()) {
                if (userDefinedInstanceProperty.getPropertyName().equals(propertyName)) {
                    foundProperty = true;
                    instanceProperties.set(userDefinedInstanceProperty, propertyValue);
                    if (!userDefinedInstanceProperty.validationPredicate().test(
                            instanceProperties.get(userDefinedInstanceProperty))) {
                        throw new IllegalArgumentException("Sleeper property: " +
                                userDefinedInstanceProperty.getPropertyName() + " is invalid");
                    }
                }
            }
            if (!foundProperty) {
                throw new IllegalArgumentException("Sleeper property: " + propertyName +
                        " does not exist and cannot be updated");
            }
            instanceProperties.saveToS3(s3Client);
        }
        System.out.println(propertyName + " has been updated to " + propertyValue);
    }

    private static void mainScreen(String message, String instanceId) throws IOException {
        clearScreen(message);
        System.out.println("ADMINISTRATION COMMAND LINE CLIENT\n----------------------------------\n");
        System.out.println("Please select from the below options and hit return:");
        System.out.println(EXIT_PROGRAM_OPTION);
        System.out.println("[1] Print Sleeper instance property report");
        System.out.println("[2] Print Sleeper table names");
        System.out.println("[3] Print Sleeper table property report");
        System.out.println("[4] Update an instance or table property\n");
        String choice = System.console().readLine("Input: ");
        switch (choice) {
            case "0":
                closeProgram();
                break;
            case "1":
                try {
                    printInstancePropertiesReport(defaultS3Client, instanceId);
                } catch (AmazonS3Exception amazonS3Exception) {
                    System.out.println(PROPERTY_LOAD_ERROR + amazonS3Exception.getMessage());
                    closeProgram();
                }
                returnToMainScreen(instanceId);
                break;
            case "2":
                try {
                    printTablesReport(defaultS3Client, instanceId);
                } catch (AmazonS3Exception amazonS3Exception) {
                    System.out.println(PROPERTY_LOAD_ERROR + amazonS3Exception.getMessage());
                    closeProgram();
                }
                returnToMainScreen(instanceId);
                break;
            case "3":
                tablePropertyReportScreen(instanceId);
                break;
            case "4":
                updatePropertyScreen("", instanceId);
                break;
            default:
                mainScreen("Input not recognised please try again\n", instanceId);
                break;
        }
    }

    private static void tablePropertyReportScreen(String instanceId) {
        try {
            client(defaultS3Client).tablePropertyReportScreen().chooseTableAndPrint(instanceId);
        } catch (UserExitedException e) {
            closeProgram();
        }
    }

    private static void updatePropertyScreen(String message, String instanceId) throws IOException {
        clearScreen(message);
        System.out.println("What is the PROPERTY NAME of the property that you would like to update?\n");
        System.out.println("Please enter the PROPERTY NAME now or use the following options:");
        System.out.println(EXIT_PROGRAM_OPTION);
        System.out.println(MAIN_MENU_OPTION);
        String choice = System.console().readLine("Input: ");
        switch (choice) {
            case "0":
                closeProgram();
                break;
            case "1":
                mainScreen("", instanceId);
                break;
            case "":
                System.out.println();
                updatePropertyScreen(INPUT_EMPTY, instanceId);
                break;
            default:
                updatePropertySecondScreen("", instanceId, choice);
                break;
        }
    }

    private static void updatePropertySecondScreen(String message, String instanceId, String propertyName) throws IOException {
        clearScreen(message);
        System.out.println("What is the new PROPERTY VALUE?\n");
        System.out.println("Please enter the PROPERTY VALUE now or use the following options:");
        System.out.println(EXIT_PROGRAM_OPTION);
        System.out.println(MAIN_MENU_OPTION);
        String choice = System.console().readLine("Input: ");
        switch (choice) {
            case "0":
                closeProgram();
                break;
            case "1":
                mainScreen("", instanceId);
                break;
            case "":
                updatePropertySecondScreen(INPUT_EMPTY, instanceId, propertyName);
                break;
            default:
                if (propertyName.startsWith("sleeper.table.")) {
                    updatePropertyThirdScreen("", instanceId, propertyName, choice);
                } else {
                    try {
                        updateProperty(defaultS3Client, instanceId, propertyName, choice, null);
                    } catch (AmazonS3Exception amazonS3Exception) {
                        System.out.println(PROPERTY_LOAD_ERROR + amazonS3Exception.getMessage());
                        closeProgram();
                    } catch (IllegalArgumentException illegalArgumentException) {
                        System.out.println(PROPERTY_ERROR + illegalArgumentException.getMessage());
                        returnToPropertyScreen(instanceId);
                    }
                    returnToMainScreen(instanceId);
                }
                break;
        }
    }

    private static void updatePropertyThirdScreen(String message, String instanceId, String propertyName, String propertyValue) throws IOException {
        clearScreen(message);
        System.out.println("As the property name begins with sleeper.table we also need to know the TABLE you want to update\n");
        System.out.println("Please enter the TABLE NAME now or use the following options:");
        System.out.println(EXIT_PROGRAM_OPTION);
        System.out.println(MAIN_MENU_OPTION);
        String choice = System.console().readLine("Input: ");
        switch (choice) {
            case "0":
                closeProgram();
                break;
            case "1":
                mainScreen("", instanceId);
                break;
            case "":
                updatePropertyThirdScreen(INPUT_EMPTY, instanceId, propertyName, propertyValue);
                break;
            default:
                try {
                    updateProperty(defaultS3Client, instanceId, propertyName, propertyValue, choice);
                } catch (AmazonS3Exception amazonS3Exception) {
                    System.out.println(PROPERTY_LOAD_ERROR + amazonS3Exception.getMessage());
                    closeProgram();
                } catch (IllegalArgumentException illegalArgumentException) {
                    System.out.println(PROPERTY_ERROR + illegalArgumentException.getMessage());
                    returnToPropertyScreen(instanceId);
                }
                returnToMainScreen(instanceId);
                break;
        }
    }

    private static void clearScreen(String message) {
        System.out.print(CLEAR_CONSOLE);
        System.out.flush();
        System.out.println(message);
    }

    private static void returnToMainScreen(String instanceId) throws IOException {
        System.out.println("\n\n----------------------------------");
        System.out.println("Hit enter to return to main screen");
        System.console().readLine();
        mainScreen("", instanceId);
    }

    private static void returnToPropertyScreen(String bucketName) throws IOException {
        System.out.println("\n\n----------------------------------");
        System.out.println("Hit enter to return to the property screen so you can adjust the property and continue");
        System.console().readLine();
        updatePropertyScreen("", bucketName);
    }

    private static void closeProgram() {
        defaultS3Client.shutdown();
        System.exit(0);
    }

    public static void main(String[] args) throws IOException {
        defaultS3Client = AmazonS3ClientBuilder.defaultClient();
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }
        mainScreen("", args[0]);
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
}
