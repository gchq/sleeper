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

package sleeper.clients;

public class QueryClientTestConstants {
    private QueryClientTestConstants() {
    }

    public static final String EXACT_QUERY_OPTION = "e";
    public static final String RANGE_QUERY_OPTION = "r";
    public static final String YES_OPTION = "y";
    public static final String NO_OPTION = "n";
    public static final String EXIT_OPTION = "";
    public static final String PROMPT_QUERY_TYPE = "Exact (e) or range (r) query? \n";
    public static final String PROMPT_EXACT_KEY_LONG_TYPE = "Enter a key for row key field key of type LongType{}: \n";
    public static final String PROMPT_MIN_INCLUSIVE = "Is the minimum inclusive? (y/n) \n";
    public static final String PROMPT_MAX_INCLUSIVE = "Is the maximum inclusive? (y/n) \n";
    public static final String PROMPT_MIN_ROW_KEY_LONG_TYPE = "Enter a minimum key for row key field key of type = LongType{} - hit return for no minimum: \n";
    public static final String PROMPT_MAX_ROW_KEY_LONG_TYPE = "Enter a maximum key for row key field key of type = LongType{} - hit return for no maximum: \n";
}
