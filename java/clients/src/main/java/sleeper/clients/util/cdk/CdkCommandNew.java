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
package sleeper.clients.util.cdk;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public record CdkCommandNew(List<String> command, List<String> arguments) {

    public static Builder builder() {
        return new Builder();
    }

    public static CdkCommandNew deployArtefacts(String deploymentId, List<String> extraEcrImages) {
        return builder().deploy()
                .context("id", deploymentId)
                .context("extraEcrImages", String.join(",", extraEcrImages))
                .build();
    }

    public static CdkCommandNew deployPropertiesChange(Path propertiesFile) {
        return builder().deploy().propertiesFile(propertiesFile).build();
    }

    public static CdkCommandNew deploySystemTestStandalone(Path propertiesFile) {
        return builder().deploy().propertiesFile(propertiesFile).build();
    }

    public static CdkCommandNew deployExisting() {
        return builder().deploy().skipVersionCheck(true).build();
    }

    public static CdkCommandNew deployExistingPaused() {
        return builder().deploy().skipVersionCheck(true).deployPaused(true).build();
    }

    public static CdkCommandNew deployNew() {
        return builder().deploy().ensureNewInstance(true).build();
    }

    public static CdkCommandNew deployNewPaused() {
        return builder().deploy().ensureNewInstance(true).deployPaused(true).build();
    }

    public static CdkCommandNew destroy() {
        return builder().destroy().validate(false).build();
    }

    public CdkCommandNew withPropertiesFile(Path propertiesFile) {
        return builder().command(command).propertiesFile(propertiesFile).arguments(arguments).build();
    }

    public static final class Builder {
        private List<String> command;
        private List<String> arguments = new ArrayList<>();

        private Builder() {
        }

        public Builder deploy() {
            return command(List.of("deploy",
                    "--require-approval", "never"));
        }

        public Builder destroy() {
            return command(List.of("destroy", "--force"));
        }

        public Builder command(List<String> command) {
            this.command = command;
            return this;
        }

        public Builder arguments(List<String> arguments) {
            this.arguments.addAll(arguments);
            return this;
        }

        public Builder propertiesFile(Path propertiesFile) {
            return context("propertiesfile", propertiesFile.toString());
        }

        public Builder ensureNewInstance(boolean ensureNewInstance) {
            return context("newinstance", ensureNewInstance);
        }

        public Builder skipVersionCheck(boolean skipVersionCheck) {
            return context("skipVersionCheck", skipVersionCheck);
        }

        public Builder deployPaused(boolean deployPaused) {
            return context("deployPaused", deployPaused);
        }

        public Builder validate(boolean validate) {
            return context("validate", validate);
        }

        public Builder context(String variable, String value) {
            arguments.addAll(List.of("-c", variable + "=" + value));
            return this;
        }

        public Builder context(String variable, boolean value) {
            return context(variable, "" + value);
        }

        public CdkCommandNew build() {
            return new CdkCommandNew(command, arguments);
        }
    }
}
