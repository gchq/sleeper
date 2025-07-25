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
package sleeper.query.core.output;

import java.util.List;

/**
 * Provides information about the results from running a query. Contains details of the number of rows in the result
 * and the location of where results are stored.
 */
public class ResultsOutputInfo {
    private final long rowCount;
    private final List<ResultsOutputLocation> locations;
    private final Exception error;

    public ResultsOutputInfo(long rowCount, List<ResultsOutputLocation> locations, Exception error) {
        this.rowCount = rowCount;
        this.locations = locations;
        this.error = error;
    }

    public ResultsOutputInfo(long numberOfRows, List<ResultsOutputLocation> locations) {
        this(numberOfRows, locations, null);
    }

    public long getRowCount() {
        return rowCount;
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
                + "rowCount=" + rowCount
                + ", locations=" + locations
                + ", error=" + error
                + '}';
    }
}
