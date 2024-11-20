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
package sleeper.query.runner.output;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputConstants;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;

import java.util.ArrayList;
import java.util.List;

public class NoResultsOutput implements ResultsOutput {

    public static final String NO_RESULTS_OUTPUT = "NoResultsOutput";

    private final List<ResultsOutputLocation> outputLocations = new ArrayList<>();

    public NoResultsOutput() {
        this.outputLocations.add(new ResultsOutputLocation(ResultsOutputConstants.DESTINATION, NO_RESULTS_OUTPUT));
    }

    @Override
    public ResultsOutputInfo publish(QueryOrLeafPartitionQuery query,
            CloseableIterator<Record> results) {
        return new ResultsOutputInfo(0, outputLocations);
    }
}
