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
package sleeper.systemtest.dsl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.systemtest.dsl.instance.ScheduleRulesDriver;

public class NoScheduleRulesDriver implements ScheduleRulesDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(NoScheduleRulesDriver.class);

    @Override
    public void enableRule(SleeperScheduleRule.InstanceRule rule) {
        LOGGER.info("Requested enabling rule: {}", rule.getRuleName());
    }

    @Override
    public void disableRule(SleeperScheduleRule.InstanceRule rule) {
        LOGGER.info("Requested disabling rule: {}", rule.getRuleName());
    }

}
