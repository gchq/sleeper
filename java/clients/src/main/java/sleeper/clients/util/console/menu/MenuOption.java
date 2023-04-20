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
package sleeper.clients.util.console.menu;

import sleeper.clients.util.console.UserExitedException;

public class MenuOption implements ConsoleChoice {

    private final String description;
    private final MenuOperation operation;

    public MenuOption(String description, MenuOperation operation) {
        this.description = description;
        this.operation = operation;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void run() throws UserExitedException, InterruptedException {
        operation.run();
    }
}
