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
package sleeper.core.record;

import com.facebook.collections.ByteArray;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.core.key.Key;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Compares keys.
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_MIGHT_BE_INFEASIBLE")
public class KeyComparator implements Comparator<Key> {
    private final List<PrimitiveType> rowKeyTypes;

    public KeyComparator() {
        this.rowKeyTypes = new ArrayList<>();
    }

    public KeyComparator(List<PrimitiveType> rowKeyTypes) {
        this.rowKeyTypes = new ArrayList<>(rowKeyTypes);
    }

    public KeyComparator(PrimitiveType... rowKeyTypes) {
        this();
        for (PrimitiveType type : rowKeyTypes) {
            this.rowKeyTypes.add(type);
        }
    }

    @Override
    public int compare(Key key1, Key key2) {
        int count = 0;
        int diff;
        for (PrimitiveType type : rowKeyTypes) {
            Comparable key1Field = null;
            Comparable key2Field = null;
            if (type instanceof ByteArrayType) {
                key1Field = null == key1.get(count) ? null : ByteArray.wrap((byte[]) key1.get(count));
                key2Field = null == key2.get(count) ? null : ByteArray.wrap((byte[]) key2.get(count));
            } else {
                key1Field = (Comparable) key1.get(count);
                key2Field = (Comparable) key2.get(count);
            }
            if (null == key1Field && null != key2Field) {
                return 1;
            }
            if (null != key1Field && null == key2Field) {
                return -1;
            }
            // If both null want to return 0, but don't return that now
            // as there may be other types to compare.
            if (null == key1Field && null == key2Field) {
                continue;
            }
            diff = key1Field.compareTo(key2Field);
            if (0 != diff) {
                return diff;
            }
            count++;
        }
        return 0;
    }
}
