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
package sleeper.core.util;

import sleeper.core.iterator.AggregationFilteringIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.properties.model.CompactionMethod;

public class AggregatingWorkaroundObjectFactory {
    private final ObjectFactory inner;

    public AggregatingWorkaroundObjectFactory(ObjectFactory inner) {
        this.inner = inner;
    }

    public <T> T getObject(String className, Class<T> parentClass) throws ObjectFactoryException {
        if (parentClass.equals(SortedRecordIterator.class) && className.equalsIgnoreCase(CompactionMethod.AGGREGATION_ITERATOR_NAME)) {
            return inner.getObject(AggregationFilteringIterator.class.getName(), parentClass);
        } else {
            return inner.getObject(className, parentClass);
        }
    }

}
