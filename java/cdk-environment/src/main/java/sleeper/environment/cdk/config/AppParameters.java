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
package sleeper.environment.cdk.config;

public class AppParameters {

    public static StringParameter INSTANCE_ID = StringParameter.keyAndDefault("instanceId", "SleeperEnvironment");
    public static VpcParameter VPC = VpcParameter.vpcIdKey("vpcId");

    public static StringParameter BUILD_REPOSITORY = StringParameter.keyAndDefault("repository", "sleeper");
    public static StringParameter BUILD_FORK = StringParameter.keyAndDefault("fork", "gchq");
    public static StringParameter BUILD_BRANCH = StringParameter.keyAndDefault("branch", "main");
}
