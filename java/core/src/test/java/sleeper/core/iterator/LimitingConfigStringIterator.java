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
package sleeper.core.iterator;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.LimitingIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.util.List;

/**
 * An example iterator that limits the number of rows read.
 */
public class LimitingConfigStringIterator implements ConfigStringIterator {

    private int limit;

    @Override
    public List<String> getRequiredValueFields() {
        return List.of();
    }

    @Override
    public CloseableIterator<Row> apply(CloseableIterator<Row> input) {
        return new LimitingIterator<>(limit, input);
    }

    @Override
    public void init(String configString, Schema schema) {
        limit = Integer.parseInt(configString);
    }

}
