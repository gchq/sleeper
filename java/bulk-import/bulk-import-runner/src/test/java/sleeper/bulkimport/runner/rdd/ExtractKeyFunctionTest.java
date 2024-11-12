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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import sleeper.core.key.Key;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class ExtractKeyFunctionTest {

    @Test
    public void shouldExtractKeyWhen1RowKey() throws Exception {
        // Given
        ExtractKeyFunction extractKeyFunction = new ExtractKeyFunction(1);
        Row row = RowFactory.create(1, 2, 3);

        // When
        Tuple2<Key, Row> tuple = extractKeyFunction.call(row);

        // Then
        assertThat(tuple._1).isEqualTo(Key.create(1));
        assertThat(tuple._2).isEqualTo(row);
    }

    @Test
    public void shouldExtractKeyWhen2RowKeys() throws Exception {
        // Given
        ExtractKeyFunction extractKeyFunction = new ExtractKeyFunction(2);
        Row row = RowFactory.create(1, 2, 3);

        // When
        Tuple2<Key, Row> tuple = extractKeyFunction.call(row);

        // Then
        assertThat(tuple._1).isEqualTo(Key.create(Arrays.asList(1, 2)));
        assertThat(tuple._2).isEqualTo(row);
    }
}
