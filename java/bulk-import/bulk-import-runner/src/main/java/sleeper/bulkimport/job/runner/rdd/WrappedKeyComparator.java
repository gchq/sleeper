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
package sleeper.bulkimport.job.runner.rdd;

import sleeper.core.key.Key;
import sleeper.core.record.KeyComparator;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.PrimitiveType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


/**
 * The {@link WrappedKeyComparator} is a {@link Comparator} of Sleeper {@link Key}s
 * that sorts them in the natural way as defined by the Sleeper schema, i.e.
 * it sorts by the row keys and then the sort keys.
 */
public class WrappedKeyComparator implements Comparator<Key>, Serializable {
    private static final long serialVersionUID = 7448396149070034670L;
    private final String schemaAsString;
    private transient KeyComparator keyComparator;

    public WrappedKeyComparator(String schemaAsString) {
        this.schemaAsString = schemaAsString;
    }

    @Override
    public int compare(Key key1, Key key2) {
        if (null == keyComparator) {
            Schema schema = new SchemaSerDe().fromJson(schemaAsString);
            List<PrimitiveType> rowAndSortKeyTypes = new ArrayList<>();
            rowAndSortKeyTypes.addAll(schema.getRowKeyTypes());
            rowAndSortKeyTypes.addAll(schema.getSortKeyTypes());
            keyComparator = new KeyComparator(rowAndSortKeyTypes);
        }
        return keyComparator.compare(key1, key2);
    }
}
