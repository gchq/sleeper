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

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;

public abstract class ChooseOneTestBase {

    protected final ToStringPrintStream out = new ToStringPrintStream();
    protected final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final ChooseOne chooseOne = new ChooseOne(out.consoleOut(), in.consoleIn());

    protected Chosen<ConsoleChoice> chooseTestOption() {
        return chooseOne().chooseFrom(OPTION_ONE, OPTION_TWO);
    }

    protected Chosen<ConsoleChoice> chooseTestOptionWithMessage(String message) {
        return chooseOne().chooseWithMessageFrom(message, OPTION_ONE, OPTION_TWO);
    }

    protected ChooseOne chooseOne() {
        return chooseOne;
    }

    protected static final ConsoleChoice OPTION_ONE = ConsoleChoice.describedAs("Option 1");
    protected static final ConsoleChoice OPTION_TWO = ConsoleChoice.describedAs("Option 2");

    protected enum TestOption implements ConsoleChoice {
        ONE("Option 1"),
        TWO("Option 2");

        private final String description;

        TestOption(String description) {
            this.description = description;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
