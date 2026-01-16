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
package sleeper.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This column handle holds details of the column name, Trino type and whether it is a rowkey/sortkey/value.
 */
public class SleeperColumnHandle implements ColumnHandle {
    private final String columnName;
    private final Type columnTrinoType;
    private final SleeperColumnHandle.SleeperColumnCategory columnCategory;

    @JsonCreator
    public SleeperColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnTrinoType") Type columnTrinoType,
            @JsonProperty("columnCategory") SleeperColumnHandle.SleeperColumnCategory columnCategory) {
        this.columnName = requireNonNull(columnName);
        this.columnTrinoType = requireNonNull(columnTrinoType);
        this.columnCategory = requireNonNull(columnCategory);
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public Type getColumnTrinoType() {
        return columnTrinoType;
    }

    @JsonProperty
    public SleeperColumnHandle.SleeperColumnCategory getColumnCategory() {
        return columnCategory;
    }

    /**
     * A convenience method to express this column handle as a metdata object.
     *
     * @return the {@link ColumnMetadata} object
     */
    public ColumnMetadata toColumnMetadata() {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnTrinoType)
                .setNullable(false) // Sleeper columns cannot contain null, although the value of this field does not seem to be reflected by Trino during a SHOW COLUMNS statement.
                .setComment(Optional.of(columnCategory.name()))
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SleeperColumnHandle that = (SleeperColumnHandle) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnCategory, that.columnCategory) &&
                Objects.equals(columnTrinoType, that.columnTrinoType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, columnTrinoType, columnCategory);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnTrinoType", columnTrinoType)
                .add("columnCategory", columnCategory)
                .toString();
    }

    /**
     * An enumeration which indicates whether the column is a rowkey/sortkey/value.
     */
    public enum SleeperColumnCategory {
        ROWKEY,
        SORTKEY,
        VALUE
    }
}
