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
/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.core.schema.type;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Code utilised below is a subset of the ByteArray class from facebook.collections.
 *
 * A wrapper object for primitive byte array type.
 * Required as hashCode and equals methods required for usage of type within a HashSet.
 */
public class ByteArray implements Comparable<ByteArray> {

    private static final ByteArrayComparator BYTE_ARRAY_COMPARATOR = new ByteArrayComparator();

    private final byte[] array;

    private ByteArray(byte[] array) {
        this.array = array;
    }

    /**
     * Returns the contents of this object as a primitive byte array.
     *
     * @return the byte array
     */
    public byte[] getArray() {
        return array;
    }

    /**
     * Returns the length of the wrapped array.
     *
     * @return the length of the array
     */
    public int getLength() {
        return array.length;
    }

    /**
     * Wraps a primitive byte array.
     *
     * @param  array the primitive byte array
     * @return       the wrapped object
     */
    public static ByteArray wrap(byte[] array) {
        return new ByteArray(array);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArray)) {
            return false;
        }

        final ByteArray that = (ByteArray) o;

        return Arrays.equals(this.getArray(), that.getArray());
    }

    @Override
    public int compareTo(ByteArray o) {
        return BYTE_ARRAY_COMPARATOR.compare(this, o);
    }

    @Override
    public int hashCode() {
        return getArray() != null ? Arrays.hashCode(getArray()) : 0;
    }

    @Override
    public String toString() {
        return "ByteArray{" +
                "array=" + Arrays.toString(array) +
                '}';
    }

    /**
     * Comparator for ByteArray type.
     */
    private static class ByteArrayComparator implements Comparator<ByteArray> {
        @Override
        public int compare(ByteArray o1, ByteArray o2) {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                } else {
                    return -1;
                }
            }

            if (o2 == null) {
                return 1;
            }

            if (o1.getArray() == null) {
                if (o2.getArray() == null) {
                    return 0;
                } else {
                    return -1;
                }
            }

            if (o2.getArray() == null) {
                return 1;
            }

            int array1Length = o1.getLength();
            int array2Length = o2.getLength();

            int length = Math.min(array1Length, array2Length);

            for (int i = 0; i < length; i++) {
                if (o1.getArray()[i] < o2.getArray()[i]) {
                    return -1;
                } else if (o1.getArray()[i] > o2.getArray()[i]) {
                    return 1;
                }
            }

            if (array1Length < array2Length) {
                return -1;
            } else if (array1Length > array2Length) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
