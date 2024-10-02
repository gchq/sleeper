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
package sleeper.build.uptime.lambda;

import java.util.List;

public class BuildUptimeEvent {

    private final String operation;
    private final List<String> ec2Ids;
    private final List<String> rules;

    private BuildUptimeEvent(String operation, List<String> ec2Ids, List<String> rules) {
        this.operation = operation;
        this.ec2Ids = ec2Ids;
        this.rules = rules;
    }

    public String getOperation() {
        return operation;
    }

    public List<String> getEc2Ids() {
        return ec2Ids;
    }

    public List<String> getRules() {
        return rules;
    }

    @Override
    public String toString() {
        return "BuildUptimeEvent{operation=" + operation + ", ec2Ids=" + ec2Ids + ", rules=" + rules + "}";
    }

    public static BuildUptimeEvent startEc2sById(String... ec2Ids) {
        return new BuildUptimeEvent("start", List.of(ec2Ids), null);
    }

    public static BuildUptimeEvent stopEc2sById(String... ec2Ids) {
        return new BuildUptimeEvent("stop", List.of(ec2Ids), null);
    }

    public static BuildUptimeEvent startRulesByName(String... rules) {
        return new BuildUptimeEvent("start", null, List.of(rules));
    }

    public static BuildUptimeEvent operation(String operation) {
        return new BuildUptimeEvent(operation, null, null);
    }
}
