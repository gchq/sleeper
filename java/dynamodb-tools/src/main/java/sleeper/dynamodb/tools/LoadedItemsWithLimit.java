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

package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LoadedItemsWithLimit {

    private final List<Map<String, AttributeValue>> items;
    private final boolean moreItems;

    public LoadedItemsWithLimit(List<Map<String, AttributeValue>> items, boolean moreItems) {
        this.items = items;
        this.moreItems = moreItems;
    }

    public List<Map<String, AttributeValue>> getItems() {
        return items;
    }

    public boolean isMoreItems() {
        return moreItems;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoadedItemsWithLimit that = (LoadedItemsWithLimit) o;
        return moreItems == that.moreItems && Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items, moreItems);
    }

    @Override
    public String toString() {
        return "LoadedItemsWithLimit{" +
                "items=" + items +
                ", moreItems=" + moreItems +
                '}';
    }
}
