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

import java.util.Objects;

/**
 * Provides information about the locaation of query results and the type of location.
 */
public class ResultsOutputLocation {
    private final String type;
    private final String location;

    public ResultsOutputLocation(String type, String location) {
        this.type = type;
        this.location = location;
    }

    public String getType() {
        return this.type;
    }

    public String getLocation() {
        return this.location;
    }

    @Override
    public String toString() {
        return "ResultsOutputLocation{"
                + "type=" + type
                + ", location=" + location
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, location);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ResultsOutputLocation)) {
            return false;
        }
        ResultsOutputLocation other = (ResultsOutputLocation) obj;
        return Objects.equals(type, other.type) && Objects.equals(location, other.location);
    }

}
