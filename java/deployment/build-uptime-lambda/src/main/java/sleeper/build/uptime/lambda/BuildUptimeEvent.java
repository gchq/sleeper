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
package sleeper.build.uptime.lambda;

import java.util.List;

public class BuildUptimeEvent {

    private final String operation;
    private final String condition;
    private final String testBucket;
    private final List<String> ec2Ids;
    private final List<String> rules;

    private BuildUptimeEvent(Builder builder) {
        operation = builder.operation;
        condition = builder.condition;
        testBucket = builder.testBucket;
        ec2Ids = builder.ec2Ids;
        rules = builder.rules;
    }

    public String getOperation() {
        return operation;
    }

    public String getCondition() {
        return condition;
    }

    public String getTestBucket() {
        return testBucket;
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

    public static Builder start() {
        return operation("start");
    }

    public static Builder stop() {
        return operation("stop");
    }

    public static Builder operation(String operation) {
        return new Builder().operation(operation);
    }

    public static class Builder {

        private String operation;
        private String condition;
        private String testBucket;
        private List<String> ec2Ids;
        private List<String> rules;

        private Builder() {
        }

        public Builder operation(String operation) {
            this.operation = operation;
            return this;
        }

        public Builder condition(String condition) {
            this.condition = condition;
            return this;
        }

        public Builder testBucket(String testBucket) {
            this.testBucket = testBucket;
            return this;
        }

        public Builder ec2Ids(List<String> ec2Ids) {
            this.ec2Ids = ec2Ids;
            return this;
        }

        public Builder rules(List<String> rules) {
            this.rules = rules;
            return this;
        }

        public Builder ec2Ids(String... ec2Ids) {
            return ec2Ids(List.of(ec2Ids));
        }

        public Builder rules(String... rules) {
            return rules(List.of(rules));
        }

        public Builder ifTestFinishedFromToday() {
            return condition(BuildUptimeCondition.TEST_FINISHED_FROM_TODAY);
        }

        public BuildUptimeEvent build() {
            return new BuildUptimeEvent(this);
        }
    }
}
