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
package sleeper.clients.admin.properties;

import sleeper.core.properties.PropertyGroup;

public class PropertyGroupWithCategory {
    public static final Category INSTANCE = Category.INSTANCE;
    public static final Category TABLE = Category.TABLE;

    private final PropertyGroup group;
    private final Category category;

    public PropertyGroupWithCategory(PropertyGroup group, Category category) {
        this.group = group;
        this.category = category;
    }

    public PropertyGroup getGroup() {
        return group;
    }

    public boolean isInstancePropertyGroup() {
        return category == Category.INSTANCE;
    }

    public boolean isTablePropertyGroup() {
        return category == Category.TABLE;
    }

    public enum Category {
        INSTANCE("Instance Properties"), TABLE("Table Properties");

        private final String name;

        Category(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
