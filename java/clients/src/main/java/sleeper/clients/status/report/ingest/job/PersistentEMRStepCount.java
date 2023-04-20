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

package sleeper.clients.status.report.ingest.job;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistentEMRStepCount {
    private PersistentEMRStepCount() {
    }

    public static Map<String, Integer> from(AmazonElasticMapReduce emrClient) {
        return new HashMap<>();
    }

    private static Map<String, Integer> countStepsByState(List<StepSummary> steps) {
        Map<String, Integer> counts = new HashMap<>();
        for (StepSummary step : steps) {
            counts.compute(step.getStatus().getState(), PersistentEMRStepCount::incrementCount);
        }
        return counts;
    }

    private static Integer incrementCount(String key, Integer countBefore) {
        if (countBefore == null) {
            return 1;
        } else {
            return countBefore + 1;
        }
    }
}
