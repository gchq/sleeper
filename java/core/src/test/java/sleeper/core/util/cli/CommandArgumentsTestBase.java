/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.util.cli;

import java.util.List;

public abstract class CommandArgumentsTestBase {

    private CommandLineUsage.Builder usageBuilder = CommandLineUsage.builder();

    protected void setPositionalArguments(String... names) {
        usageBuilder.positionalArguments(List.of(names));
    }

    protected void setSystemArguments(String... names) {
        usageBuilder.systemArguments(List.of(names));
    }

    protected void setOptions(CommandOption... options) {
        usageBuilder.options(List.of(options));
    }

    protected void setHelpSummary(String helpSummary) {
        usageBuilder.helpSummary(helpSummary);
    }

    protected void setPassThroughExtraArguments(boolean setPassThroughExtraArguments) {
        usageBuilder.passThroughExtraArguments(setPassThroughExtraArguments);
    }

    protected CommandArguments parse(String... args) {
        return CommandArgumentReader.parse(usage(), args);
    }

    protected String usageMessage() {
        return usage().createUsageMessage();
    }

    protected String helpText() {
        return usage().createHelpText();
    }

    protected CommandLineUsage usage() {
        return usageBuilder.build();
    }
}
