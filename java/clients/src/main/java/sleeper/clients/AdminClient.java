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
import com.amazonaws.services.s3.model.*;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.table.job.TableLister;

import java.io.IOException;
import java.util.*;

public class AdminClient {
    private final static String CLEAR_CONSOLE = "\033[H\033[2J";
    private final static String INPUT_EMPTY = "\nYou did not enter anything please try again\n";
    private final static String PROPERTY_LOAD_ERROR =
            "There has been a problem loading or changing properties from/in the S3 bucket. \nThe error message was: ";
    private final static String TABLE_NULL_ERROR = "When a table property is being updated e.g. sleeper.table.* " +
            "then a Table Name must be provided in the parameters";
    private final static String PROPERTY_ERROR = "A property update error has occurred either the property name " +
            "or value is invalid. \nThe error message was: ";
    private final static String EXIT_PROGRAM_OPTION = "[0] Exit program";
    private final static String MAIN_MENU_OPTION = "[1] Return to Main Menu\n";

    private static AmazonS3 defaultS3Client;

    static void printInstancePropertiesReport(AmazonS3 s3Client, String instanceId) throws IOException, AmazonS3Exception {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

        Iterator<Map.Entry<Object, Object>> propertyIterator = instanceProperties.getPropertyIterator();
        TreeMap<Object, Object> instancePropertyTreeMap = new TreeMap<>();
        while (propertyIterator.hasNext()) {
            Map.Entry<Object, Object> mapElement = propertyIterator.next();
            instancePropertyTreeMap.put(mapElement.getKey(), mapElement.getValue());
        }
        for (UserDefinedInstanceProperty userDefinedInstanceProperty : UserDefinedInstanceProperty.values()) {
            if (!instancePropertyTreeMap.containsKey(userDefinedInstanceProperty.getPropertyName())) {
                instancePropertyTreeMap.put(userDefinedInstanceProperty.getPropertyName(), instanceProperties.get(userDefinedInstanceProperty));
            }
        }
        System.out.println("\n\n Instance Property Report \n -------------------------");
        for (Map.Entry<Object, Object> entry : instancePropertyTreeMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    static void printTablePropertiesReport(AmazonS3 s3Client, String instanceId, String tableName) throws IOException, AmazonS3Exception {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

        TablePropertiesProvider tablePropertiesProvider =
                new TablePropertiesProvider(s3Client, instanceProperties);
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);

        Iterator<Map.Entry<Object, Object>> propertyIterator = tableProperties.getPropertyIterator();
        TreeMap<Object, Object> tablePropertyTreeMap = new TreeMap<>();
        while (propertyIterator.hasNext()) {
            Map.Entry<Object, Object> mapElement = propertyIterator.next();
            tablePropertyTreeMap.put(mapElement.getKey(), mapElement.getValue());
        }
        for (TableProperty tableProperty : TableProperty.values()) {
            if (!tablePropertyTreeMap.containsKey(tableProperty.getPropertyName())) {
                tablePropertyTreeMap.put(tableProperty.getPropertyName(), tableProperties.get(tableProperty));
            }
        }
        System.out.println("\n\n Table Property Report \n -------------------------");
        for (Map.Entry<Object, Object> entry : tablePropertyTreeMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    static void printTablesReport(AmazonS3 s3Client, String instanceId) throws IOException, AmazonS3Exception {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

        TableLister tableLister = new TableLister(s3Client, instanceProperties);
        List<String> tableList = tableLister.listTables();

        System.out.println("\n\n Table Names Report \n -------------------------");
        for (String tableName : tableList) {
            System.out.println(tableName);
        }
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
                tablePropertyReportScreen("", instanceId);
                break;
            case "4":
                updatePropertyScreen("", instanceId);
                break;
            default:
                mainScreen("Input not recognised please try again\n", instanceId);
                break;
        }
    }

    private static void tablePropertyReportScreen(String message, String instanceId) throws IOException {
        clearScreen(message);
        System.out.println("Which TABLE do you want to check?\n");
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
                tablePropertyReportScreen(INPUT_EMPTY, instanceId);
                break;
            default:
                try {
                    printTablePropertiesReport(defaultS3Client, instanceId, choice);
                } catch (AmazonS3Exception amazonS3Exception) {
                    System.out.println(PROPERTY_LOAD_ERROR + amazonS3Exception.getMessage());
                    closeProgram();
                }
                returnToMainScreen(instanceId);
                break;
        }
    }

    private static void updatePropertyScreen(String message, String instanceId) throws IOException{
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

    private static void updatePropertySecondScreen(String message, String instanceId, String propertyName) throws IOException{
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
}