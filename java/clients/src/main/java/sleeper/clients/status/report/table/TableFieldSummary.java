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
package sleeper.clients.status.report.table;

class TableFieldSummary {

    private final TableField field;
    private final int maxValueLength;
    private final boolean visible;

    private TableFieldSummary(TableField field, int maxValueLength, boolean visible) {
        this.field = field;
        this.maxValueLength = maxValueLength;
        this.visible = visible;
    }

    static TableFieldSummary hidden(TableField field) {
        return new TableFieldSummary(field, 0, false);
    }

    static TableFieldSummary visibleWithMaxValueLength(TableField field, int maxValueLength) {
        return new TableFieldSummary(field, maxValueLength, true);
    }

    public int getMaxValueLength() {
        return maxValueLength;
    }

    public boolean isVisible() {
        return visible;
    }

    public TableFieldDefinition.HorizontalAlignment getHorizontalAlignment() {
        return field.getHorizontalAlignment();
    }

    public int getIndex() {
        return field.getIndex();
    }
}
