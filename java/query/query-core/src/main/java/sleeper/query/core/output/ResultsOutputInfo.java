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
package sleeper.query.core.output;

import java.util.List;

public class ResultsOutputInfo {
    private final long recordCount;
    private final List<ResultsOutputLocation> locations;
    private final Exception error;

    public ResultsOutputInfo(long recordCount, List<ResultsOutputLocation> locations, Exception error) {
        this.recordCount = recordCount;
        this.locations = locations;
        this.error = error;
    }

    public ResultsOutputInfo(long recordCount, List<ResultsOutputLocation> locations) {
        this(recordCount, locations, null);
    }

    public long getRecordCount() {
        return recordCount;
    }

    public List<ResultsOutputLocation> getLocations() {
        return locations;
    }

    public Exception getError() {
        return error;
    }

    @Override
    public String toString() {
        return "ResultsOutputInfo{"
                + "recordCount=" + recordCount
                + ", locations=" + locations
                + ", error=" + error
                + '}';
    }
}
