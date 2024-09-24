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
package sleeper.clients.admin.testutils;

import sleeper.configuration.properties.instance.InstancePropertyGroup;
import sleeper.configuration.properties.table.TablePropertyGroup;
import sleeper.core.properties.PropertyGroup;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;

public class ExpectedAdminConsoleValues {

    private ExpectedAdminConsoleValues() {
    }

    public static final String MAIN_SCREEN = "\n" +
            "ADMINISTRATION COMMAND LINE CLIENT\n" +
            "----------------------------------\n" +
            "\n" +
            "Please select from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] View/edit instance configuration\n" +
            "[2] View/edit table configuration\n" +
            "[3] View configuration by group\n" +
            "[4] Print Sleeper table names\n" +
            "[5] Run partition status report\n" +
            "[6] Run files status report\n" +
            "[7] Run compaction status report\n" +
            "[8] Run ingest status report\n" +
            "[9] Run ingest batcher report\n" +
            "\n" +
            "Input: \n";
    private static final List<PropertyGroup> INSTANCE_PROPERTY_GROUPS = InstancePropertyGroup.getAll();
    private static final List<PropertyGroup> TABLE_PROPERTY_GROUPS = TablePropertyGroup.getAll();
    private static final int TABLE_START_INDEX = INSTANCE_PROPERTY_GROUPS.size() + 2;
    public static final String GROUP_SELECT_SCREEN = "\n" +
            "Please select a group from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            IntStream.range(0, INSTANCE_PROPERTY_GROUPS.size())
                    .mapToObj(index -> String.format("[%d] %s - %s%n", index + 2, "Instance Properties",
                            INSTANCE_PROPERTY_GROUPS.get(index).getName()))
                    .collect(Collectors.joining())
            + IntStream.range(0, TABLE_PROPERTY_GROUPS.size())
                    .mapToObj(index -> String.format("[%d] %s - %s%n", index + TABLE_START_INDEX, "Table Properties",
                            TABLE_PROPERTY_GROUPS.get(index).getName()))
                    .collect(Collectors.joining())
            + "\n" +
            "Input: \n";

    public static String instancePropertyGroupOption(PropertyGroup group) {
        return "" + (InstancePropertyGroup.getAll().indexOf(group) + 2);
    }

    public static String tablePropertyGroupOption(PropertyGroup group) {
        return "" + (TablePropertyGroup.getAll().indexOf(group) + TABLE_START_INDEX);
    }

    public static final String TABLE_SELECT_SCREEN = "\n" +
            "Which TABLE do you want to select?\n" +
            "\n" +
            "Please enter the TABLE NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n" +
            "Input: \n";
    public static final String PROPERTY_SAVE_CHANGES_SCREEN = "" +
            "Please select from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] Save changes\n" +
            "[2] Return to editor\n" +
            "[3] Discard changes and return to main menu\n" +
            "\n" +
            "Input: \n";
    public static final String PROPERTY_VALIDATION_SCREEN = "" +
            "Please select from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] Return to editor\n" +
            "[2] Discard changes and return to main menu\n" +
            "\n" +
            "Input: \n";

    public static final String COMPACTION_STATUS_STORE_NOT_ENABLED_MESSAGE = "" +
            "\nCompaction status store not enabled. Please enable in instance properties to access this screen\n";
    public static final String INGEST_STATUS_STORE_NOT_ENABLED_MESSAGE = "" +
            "\nIngest status store not enabled. Please enable in instance properties to access this screen\n";

    public static final String EXIT_OPTION = "0";
    public static final String RETURN_TO_MAIN_SCREEN_OPTION = "1";
    public static final String INSTANCE_CONFIGURATION_OPTION = "1";
    public static final String TABLE_CONFIGURATION_OPTION = "2";
    public static final String CONFIGURATION_BY_GROUP_OPTION = "3";
    public static final String TABLE_NAMES_REPORT_OPTION = "4";
    public static final String PARTITION_STATUS_REPORT_OPTION = "5";
    public static final String FILES_STATUS_REPORT_OPTION = "6";
    public static final String COMPACTION_STATUS_REPORT_OPTION = "7";
    public static final String INGEST_STATUS_REPORT_OPTION = "8";
    public static final String INGEST_BATCHER_REPORT_OPTION = "9";
    public static final String COMPACTION_JOB_STATUS_REPORT_OPTION = "1";
    public static final String COMPACTION_TASK_STATUS_REPORT_OPTION = "2";

    public static final String JOB_QUERY_ALL_OPTION = "1";
    public static final String JOB_QUERY_UNFINISHED_OPTION = "2";
    public static final String JOB_QUERY_DETAILED_OPTION = "3";
    public static final String JOB_QUERY_RANGE_OPTION = "4";
    public static final String JOB_QUERY_REJECTED_OPTION = "5";

    public static final String TASK_QUERY_ALL_OPTION = "1";
    public static final String TASK_QUERY_UNFINISHED_OPTION = "2";
    public static final String INGEST_JOB_STATUS_REPORT_OPTION = "1";
    public static final String INGEST_TASK_STATUS_REPORT_OPTION = "2";
    public static final String BATCHER_QUERY_ALL_OPTION = "1";
    public static final String BATCHER_QUERY_PENDING_OPTION = "2";
    public static final String PROMPT_INPUT_NOT_RECOGNISED = "\nInput not recognised please try again\n";

    public static final class SaveChangesScreen {
        public static final String SAVE_CHANGES_OPTION = "1";
        public static final String RETURN_TO_EDITOR_OPTION = "2";
        public static final String DISCARD_CHANGES_OPTION = "3";
    }

    public static final class ValidateChangesScreen {
        public static final String RETURN_TO_EDITOR_OPTION = "1";
        public static final String DISCARD_CHANGES_OPTION = "2";
    }

    public static final String PROMPT_RETURN_TO_MAIN = "" +
            "\n\n----------------------------------\n" +
            "Hit enter to return to main screen\n";

    public static final String PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN = "" +
            "\n\n----------------------------------\n" +
            "Saved successfully, hit enter to return to main screen\n";

    public static final String DISPLAY_MAIN_SCREEN = CLEAR_CONSOLE + MAIN_SCREEN;
}
