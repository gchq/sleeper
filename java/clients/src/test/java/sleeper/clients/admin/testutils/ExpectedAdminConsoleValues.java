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
package sleeper.clients.admin.testutils;

public class ExpectedAdminConsoleValues {

    private ExpectedAdminConsoleValues() {
    }

    public static final String MAIN_SCREEN = "\n" +
            "ADMINISTRATION COMMAND LINE CLIENT\n" +
            "----------------------------------\n" +
            "\n" +
            "Please select from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] Print Sleeper instance property report\n" +
            "[2] Print Sleeper table names\n" +
            "[3] Print Sleeper table property report\n" +
            "[4] Update an instance or table property\n" +
            "[5] Run partition status report\n" +
            "[6] Run compaction status report\n" +
            "\n" +
            "Input: \n";

    public static final String TABLE_SELECT_SCREEN = "\n" +
            "Which TABLE do you want to select?\n" +
            "\n" +
            "Please enter the TABLE NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n" +
            "Input: \n";

    public static final String UPDATE_PROPERTY_SCREEN = "\n" +
            "What is the PROPERTY NAME of the property that you would like to update?\n" +
            "\n" +
            "Please enter the PROPERTY NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n" +
            "Input: \n";

    public static final String UPDATE_PROPERTY_ENTER_VALUE_SCREEN = "\n" +
            "What is the new PROPERTY VALUE?\n" +
            "\n" +
            "Please enter the PROPERTY VALUE now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n" +
            "Input: \n";

    public static final String UPDATE_PROPERTY_ENTER_TABLE_SCREEN = "\n" +
            "As the property name begins with sleeper.table we also need to know the TABLE you want to update\n" +
            "\n" +
            "Please enter the TABLE NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n" +
            "Input: \n";

    public static final String EXIT_OPTION = "0";
    public static final String RETURN_TO_MAIN_SCREEN_OPTION = "1";
    public static final String INSTANCE_PROPERTY_REPORT_OPTION = "1";
    public static final String TABLE_NAMES_REPORT_OPTION = "2";
    public static final String TABLE_PROPERTY_REPORT_OPTION = "3";
    public static final String UPDATE_PROPERTY_OPTION = "4";
    public static final String PARTITION_STATUS_REPORT_OPTION = "5";
    public static final String COMPACTION_STATUS_REPORT_OPTION = "6";
    public static final String COMPACTION_JOB_STATUS_REPORT_OPTION = "1";
    public static final String COMPACTION_TASK_STATUS_REPORT_OPTION = "2";

    public static final String JOB_QUERY_ALL_OPTION = "1";
    public static final String JOB_QUERY_UNKNOWN_OPTION = "2";
    public static final String JOB_QUERY_DETAILED_OPTION = "3";
    public static final String JOB_QUERY_RANGE_OPTION = "4";

    public static final String TASK_QUERY_ALL_OPTION = "1";
    public static final String TASK_QUERY_UNFINISHED_OPTION = "2";
    public static final String PROMPT_INPUT_NOT_RECOGNISED = "\nInput not recognised please try again\n";

    public static final String PROMPT_RETURN_TO_MAIN = "" +
            "\n\n----------------------------------\n" +
            "Hit enter to return to main screen\n";

    public static final String PROMPT_RETURN_TO_PROPERTY = "" +
            "\n\n----------------------------------\n" +
            "Hit enter to return to the property screen so you can adjust the property and continue\n";
}
