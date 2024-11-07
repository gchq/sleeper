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
package sleeper.bulkimport.runner.rdd;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import sleeper.core.key.Key;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts the first n columns to create a Sleeper key.
 */
public class ExtractKeyFunction implements PairFunction<Row, Key, Row> {
    private static final long serialVersionUID = 4328608066452390263L;

    private final int numRowKeys;

    public ExtractKeyFunction(int numRowKeys) {
        this.numRowKeys = numRowKeys;
    }

    @Override
    public Tuple2<Key, Row> call(Row row) {
        List<Object> keys = new ArrayList<>(numRowKeys);
        for (int i = 0; i < numRowKeys; i++) {
            keys.add(row.get(i));
        }
        Key key = Key.create(keys);
        return new Tuple2<>(key, row);
    }
}
