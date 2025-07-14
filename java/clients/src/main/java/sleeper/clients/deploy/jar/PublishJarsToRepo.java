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
package sleeper.clients.deploy.jar;

import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;

public class PublishJarsToRepo {

    private PublishJarsToRepo() {
    }

    public static void main(String[] args) throws Exception {
        //Check arg 1 is apropriate url
        /*
         * TEMP Removed until regex decided
         * if (!args[1].matches("SOME URL REGEX")) {
         * System.out.println("Error: [" + args[1] + "] did not match the expected url pattern");
         * return;
         * }
         */

        CommandPipelineRunner runner = CommandUtils::runCommandInheritIO;
        runner.run("java", "--version");
        //For each LambdaJar/ClientJar
        //Upload to repo
    }

}
