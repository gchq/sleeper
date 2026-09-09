/*
 * Copyright 2022-2026 Crown Copyright
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

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
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

    @Test
    void shouldPushOrFilterWithNestedOrAndInBranchesOnKeyField() {
        // Given
        // (key IN {"A", "B"} OR key = "C") OR key = "D"
        Or or = new Or(
                new Or(new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B"}), new EqualTo(ROW_KEY_FIELD.getName(), "C")),
                new EqualTo(ROW_KEY_FIELD.getName(), "D"));
        Filter[] filters = new Filter[]{or};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactly(or);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).isEmpty();
    }

    @Test
    void shouldPushOrFilterWithAndBranchOnKeyField() {
        // Given
        // "(key > "A" AND key < "E") OR key = G"
        Or or = new Or(
                new And(new GreaterThan(ROW_KEY_FIELD.getName(), "A"), new LessThan(ROW_KEY_FIELD.getName(), "E")),
                new EqualTo(ROW_KEY_FIELD.getName(), "G"));
        Filter[] filters = new Filter[]{or};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactly(or);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).isEmpty();
    }

    @Test
    void shouldPushAndFilterOnKeyField() {
        // Given
        And and = new And(new GreaterThan(ROW_KEY_FIELD.getName(), "A"), new LessThan(ROW_KEY_FIELD.getName(), "E"));
        Filter[] filters = new Filter[]{and};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).containsExactly(and);
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).isEmpty();
    }

    @Test
    void shouldNotPushAndFilterWhereOneBranchIsNotOnKeyField() {
        // Given
        // Due to the value field predicate, this is not applied
        And and = new And(new EqualTo(ROW_KEY_FIELD.getName(), "A"), new EqualTo("value", "B"));
        Or or = new Or(and, new EqualTo(ROW_KEY_FIELD.getName(), "C"));
        Filter[] filters = new Filter[]{and, or};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).isEmpty();
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).containsExactly(and, or);
    }

    @Test
    void shouldNotPushNotFilterOrOrFilterWithNotBranch() {
        // Given
        // Not filters cannot be pushed down, including if they are in an or
        Not not = new Not(new EqualTo(ROW_KEY_FIELD.getName(), "A"));
        Or or = new Or(new Not(new EqualTo(ROW_KEY_FIELD.getName(), "B")), new EqualTo(ROW_KEY_FIELD.getName(), "C"));
        Filter[] filters = new Filter[]{not, or};
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(SCHEMA);

        // When
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);

        // Then
        assertThat(pushedAndNonPushedFilters.getPushedFilters()).isEmpty();
        assertThat(pushedAndNonPushedFilters.getNonPushedFilters()).containsExactly(not, or);
    }
}
