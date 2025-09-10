/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.common.task;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate task for each, up to a
 * configurable maximum.
 */
public class RunBulkExportTasks {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: <instance-id> <number-of-tasks>");
            return;
        }
        RunDataProcessingTasks.run(args[0], Integer.parseInt(args[1]), false);
    }

    private RunBulkExportTasks() {
    }
}
