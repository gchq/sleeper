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
package sleeper.configuration.properties.table;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.jupiter.api.Test;

import sleeper.core.table.TableId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3TablePropertiesStoreIT extends TablePropertiesITBase {

    @Test
    void shouldSaveToS3() {
        // When
        store.save(tableProperties);

        // Then
        assertThat(store.loadProperties(tableProperties.getId()))
                .isEqualTo(tableProperties);
    }

    @Test
    void shouldFindNoTable() {
        assertThatThrownBy(() -> store.loadProperties(TableId.uniqueIdAndName("not-a-table", "not-a-name")))
                .isInstanceOf(AmazonS3Exception.class);
    }
}
