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
package sleeper.spark;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.spark.FindFiltersToPush.PushedAndNonPushedFilters;

import static org.assertj.core.api.Assertions.assertThat;

public class FindFiltersToPushTest {
    private static final Field ROW_KEY_FIELD = new Field("key", new StringType());
    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(ROW_KEY_FIELD)
            .valueFields(new Field("value", new StringType()))
            .build();

    @Test
    void shouldIgnoreFiltersOnValueField() {
        // Given
        GreaterThan greaterThan = new GreaterThan("value", "g");
        Filter[] filters = new Filter[]{greaterThan};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).isEmpty();
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).containsExactly(greaterThan);
    }

    @Test
    void shouldPushFilterOnKeyField() {
        // Given
        LessThan lessThan = new LessThan("key", "h");
        GreaterThan greaterThan = new GreaterThan("value", "g");
        Filter[] filters = new Filter[]{lessThan, greaterThan};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactly(lessThan);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).containsExactly(greaterThan);
    }

    @Test
    void shouldPushInFilterOnKeyField() {
        // Given
        In in = new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B", "C"});
        GreaterThan greaterThan = new GreaterThan("value", "g");
        Filter[] filters = new Filter[]{in, greaterThan};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactly(in);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).containsExactly(greaterThan);
    }

    @Test
    void shouldPushOrFilterWhereLeftAndRightAreOnKeyField() {
        // Given
        In in = new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B", "C"});
        GreaterThan greaterThan = new GreaterThan(ROW_KEY_FIELD.getName(), "g");
        Filter[] filters = new Filter[]{in, greaterThan};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactlyInAnyOrder(in, greaterThan);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).isEmpty();
    }

    @Test
    void shouldNotPushOrFilterWhereOneIsNotOnKeyField() {
        // Given
        In in = new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B", "C"});
        GreaterThan greaterThan = new GreaterThan("value", "g");
        Filter[] filters = new Filter[]{in, greaterThan};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactly(in);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).containsExactly(greaterThan);
    }
}
