/*
 * Copyright 2022 Crown Copyright
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

import java.io.Serializable;
import java.util.Comparator;

import sleeper.core.key.Key;
import sleeper.core.record.KeyComparator;
import sleeper.core.schema.SchemaSerDe;

/**
 * The {@link WrappedKeyComparator} is a {@link Comparator} of Sleeper {@link Key}s
 * that sorts them in the natural way as defined by the Sleeper schema.
 */
public class WrappedKeyComparator implements Comparator<Key>, Serializable {
    private final String schemaAsString;
    private transient KeyComparator keyComparator;
    
    public WrappedKeyComparator(String schemaAsString) {
        this.schemaAsString = schemaAsString;
    }

    @Override
    public int compare(Key key1, Key key2) {
        if (null == keyComparator) {
            keyComparator = new KeyComparator(new SchemaSerDe().fromJson(schemaAsString).getRowKeyTypes());
        }
        return keyComparator.compare(key1, key2);
    }
}
