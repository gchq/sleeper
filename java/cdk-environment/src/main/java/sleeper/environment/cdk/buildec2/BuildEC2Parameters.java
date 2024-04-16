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
package sleeper.environment.cdk.buildec2;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.StringParameter;

public class BuildEC2Parameters {

    public static final StringParameter REPOSITORY = AppParameters.BUILD_REPOSITORY;
    public static final StringParameter FORK = AppParameters.BUILD_FORK;
    public static final StringParameter BRANCH = AppParameters.BUILD_BRANCH;

    private final String repository;
    private final String fork;
    private final String branch;
    private final BuildEC2Image image;

    private BuildEC2Parameters(AppContext context) {
        repository = context.get(REPOSITORY);
        fork = context.get(FORK);
        branch = context.get(BRANCH);
        image = BuildEC2Image.from(context);
    }

    static BuildEC2Parameters from(AppContext context) {
        return new BuildEC2Parameters(context);
    }

    String fillUserDataTemplate(String template) {
        return template.replace("${repository}", repository)
                .replace("${fork}", fork)
                .replace("${branch}", branch)
                .replace("${loginUser}", image.loginUser());
    }

    BuildEC2Image image() {
        return image;
    }

}
