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
package sleeper.core.key;

import com.facebook.collections.ByteArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper for a {@link List} of {@link Object}s used as a key.
 */
public class Key {
    private List<Object> key;

    private Key(List<Object> key) {
        this.key = key;
    }

    public Object get(int i) {
        return key.get(i);
    }

    public List<Object> getKeys() {
        return key;
    }

    public int size() {
        return key.size();
    }

    public boolean isEmpty() {
        return key.isEmpty();
    }

    @Override
    public int hashCode() {
        List<Object> transformedThis = cloneWithWrappedByteArray(key);
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(transformedThis);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Key other = (Key) obj;

        final List<Object> transformedThis = cloneWithWrappedByteArray(key);
        final List<Object> transformedOther = cloneWithWrappedByteArray(other.key);
        return Objects.equals(transformedThis, transformedOther);
    }

    private static List<Object> cloneWithWrappedByteArray(List<Object> input) {
        if (null == input) {
            return null;
        }
        List<Object> clone = new ArrayList<>();
        for (Object o : input) {
            if (o instanceof byte[]) {
                clone.add(ByteArray.wrap((byte[]) o));
            } else {
                clone.add(o);
            }
        }
        return clone;
    }

    @Override
    public String toString() {
        return "Key{" + cloneWithWrappedByteArray(key) + '}';
    }

    public static Key create(Object obj) {
        if (null == obj) {
            return new Key(Collections.singletonList(null));
        }
        if (obj instanceof List) {
            return new Key((List<Object>) obj);
        }
        return new Key(Collections.singletonList(obj));
    }
}
