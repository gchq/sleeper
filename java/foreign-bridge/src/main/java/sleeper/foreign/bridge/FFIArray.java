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
package sleeper.foreign.bridge;

import jnr.ffi.NativeType;
import jnr.ffi.Struct;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Array class that must be used inside a JNR-FFI Struct. Creates a dynamic array that can be
 * passed to C. Strong references are maintained for allocated memory so GC will dispose of
 * memory when this object is collected.
 *
 * @param <T> object type of array
 */
public class FFIArray<T> {
    /** Length of array. Will be transferred across FFI boundary. */
    private final Struct.size_t len;

    /** Pointer to base of dynamically allocated array. This pointer value will be transferred across FFI boundary. */
    private final Struct.Pointer arrayBase;
    /**
     * Reference to dynamically allocated array to prevent GC until Array instance
     * is collected. This pointer value will NOT be transferred across FFI boundary.
     */
    private jnr.ffi.Pointer basePtr;
    /**
     * Reference to dynamically allocated items to prevent GC until Array instance
     * is collected. These pointers will NOT be transferred across FFI boundary.
     */
    private jnr.ffi.Pointer[] items;

    public FFIArray(Struct enclosing) {
        this.len = enclosing.new size_t();
        this.arrayBase = enclosing.new Pointer();
    }

    /**
     * Array length.
     *
     * @return length of array
     */
    public long length() {
        return len.get();
    }

    /**
     * Populates this FFI compatible array with the values from the given Java array.
     *
     * Memory is directly allocated (off JVM heap) for an array of pointers
     * of length {@code arr.length}. This memory is used to store pointers
     * to each element of the FFI array. The element count and address
     * of this pointer array are stored in this object so that they can be
     * read by native code.
     *
     * @param arr          array data
     * @param nullsAllowed if null pointers are allowed in the data array
     */
    public void populate(final T[] arr, boolean nullsAllowed) {
        final jnr.ffi.Runtime r = len.struct().getRuntime();
        // Calculate size needed for array of pointers
        int ptrSize = r.findType(NativeType.ADDRESS).size();
        if (arr.length > 0) {
            int size = arr.length * ptrSize;
            // Allocate some memory for pointers
            this.basePtr = r.getMemoryManager().allocateDirect(size);
            this.arrayBase.set(basePtr);

            this.items = new jnr.ffi.Pointer[arr.length];

            for (int i = 0; i < arr.length; i++) {
                if (!nullsAllowed && arr[i] == null) {
                    throw new NullPointerException("Index " + i + " of array is null when nulls aren't allowed here");
                }
                setValue(arr[i], i, r);
            }

            // Bulk set the pointers in the base array
            this.basePtr.put(0, this.items, 0, this.items.length);
        } else {
            // Null out zero length arrays
            this.basePtr = null;
            this.items = null;
        }

        // Set length of array in struct
        this.len.set(arr.length);
    }

    /**
     * Fetches the contents of this array back into a Java array.
     *
     * The items in this array are read back from the direct heap storage
     * where they will have been allocated by
     * {@link FFIArray#populate(Object[], boolean)}.
     *
     * @param  <E>                   the generic type of elements in the array
     * @param  clazz                 the class type of generic parameter T
     * @param  nullsAllowed          if nulls are allowed in this array
     * @return                       Java array of object from this array
     * @throws IllegalStateException if this array is not valid before calling this method
     * @throws IllegalStateException if a {@code null} is found in a non-nullable array
     * @throws NullPointerException  if parts of this object are {@code null} when they
     *                               shouldn't be, see {@link FFIArray#validate()}
     */
    public <E> T[] readBack(Class<E> clazz, boolean nullsAllowed) {
        validate();
        final jnr.ffi.Runtime r = len.struct().getRuntime();
        int len = this.len.intValue();

        @SuppressWarnings("unchecked")
        T[] values = (T[]) java.lang.reflect.Array.newInstance(clazz, len);

        for (int i = 0; i < len; i++) {
            values[i] = getValue(i, clazz, nullsAllowed, r);
        }
        return values;
    }

    /**
     * Check class invariants.
     *
     * @throws IllegalStateException if a violation is found
     */
    public void validate() {
        if (len.get() == 0) {
            if (basePtr != null || items != null) {
                throw new IllegalStateException("array length is 0 but pointers not null");
            }
        } else {
            if (len.get() != items.length) {
                throw new IllegalStateException(
                        "length of " + len.get() + " doesn't match items length of " + items.length);
            }
            Objects.requireNonNull(this.basePtr, "base pointer is null");
            Objects.requireNonNull(this.items, "items array is null");
            if (this.arrayBase.get().address() != this.basePtr.address()) {
                throw new IllegalStateException("array base pointer and stored base pointer differ!");
            }
        }
    }

    /**
     * Sets a given value in the array to a specific value. The data
     * is byte encoded.
     *
     * Memory is dynamically allocated off JVM heap for the value.
     *
     * Intended for internal use only.
     *
     * @param  item                      the item to encode
     * @param  idx                       array position to use
     * @param  r                         struct runtime
     * @throws ClassCastException        if item is of wrong class
     * @throws IndexOutOfBoundsException if idx is invalid
     */
    protected void setValue(T item, int idx, jnr.ffi.Runtime r) {
        if (item == null) {
            this.items[idx] = jnr.ffi.Pointer.wrap(r, 0);
        } else if (item instanceof Integer) {
            int e = (int) item;
            this.items[idx] = r.getMemoryManager().allocateDirect(r.findType(NativeType.SINT).size());
            this.items[idx].putInt(0, e);
        } else if (item instanceof Long) {
            long e = (long) item;
            this.items[idx] = r.getMemoryManager().allocateDirect(r.findType(NativeType.SLONGLONG).size());
            this.items[idx].putLong(0, e);
        } else if (item instanceof java.lang.String) {
            // Strings are encoded as 4 byte length then value
            java.lang.String e = (java.lang.String) item;
            byte[] utf8string = e.getBytes(StandardCharsets.UTF_8);
            // Add four for length
            int stringSize = utf8string.length + 4;
            // Allocate memory for string and write length then the string
            this.items[idx] = r.getMemoryManager().allocateDirect(stringSize);
            this.items[idx].putInt(0, utf8string.length);
            this.items[idx].put(4, utf8string, 0, utf8string.length);
        } else if (item instanceof byte[]) {
            // Byte arrays are encoded as 4 byte length then value
            byte[] e = (byte[]) item;
            int byteSize = e.length + 4;
            this.items[idx] = r.getMemoryManager().allocateDirect(byteSize);
            this.items[idx].putInt(0, e.length);
            this.items[idx].put(4, e, 0, e.length);
        } else if (item instanceof Boolean) {
            boolean e = (boolean) item;
            this.items[idx] = r.getMemoryManager().allocateDirect(1);
            this.items[idx].putByte(0, e ? (byte) 1 : (byte) 0);
        } else {
            throw new ClassCastException("Can't cast " + item.getClass() + " to a valid Sleeper row key type");
        }
    }

    /**
     * Retrieves a value from the array of a Sleeper primitive type.
     *
     * @param  index                     the array index
     * @param  type                      the type
     * @param  nullsAllowed              if nulls may be present in this array
     * @param  runtime                   the runtime this array was allocated with
     * @return                           the value
     * @throws IndexOutOfBoundsException if {@code idx} is out of range
     * @throws IllegalStateException     if a pointer to 0 is found in a non-nullable array
     */
    public Object getFieldValue(int index, PrimitiveType type, boolean nullsAllowed, jnr.ffi.Runtime runtime) {
        if (type instanceof LongType) {
            return getValue(index, Long.class, nullsAllowed, runtime);
        } else if (type instanceof IntType) {
            return getValue(index, Integer.class, nullsAllowed, runtime);
        } else if (type instanceof StringType) {
            return getValue(index, String.class, nullsAllowed, runtime);
        } else if (type instanceof ByteArrayType) {
            return getValue(index, byte[].class, nullsAllowed, runtime);
        } else {
            throw new IllegalArgumentException("Unsupported primitive type: " + type);
        }
    }

    /**
     * Reads a value from array.
     *
     * The value of the array element is read from previously allocated memory. The
     * array MUST have been previously populated with {@link FFIArray#populate(Object[], boolean)}.
     * The type of the array item to read is given in the {@code clazz} argument. This should match the
     * generic type given.
     *
     * If nulls are not allowed in this array, but a null pointer is found, then an exception is thrown.
     *
     * @param  <E>                       the generic type of elements in the array
     * @param  idx                       the index to read
     * @param  clazz                     the class type of generic parameter E
     * @param  nullsAllowed              if nulls may be present in this array
     * @param  r                         the runtime this array was allocated with
     * @return                           the array element, or {@code null} if nulls are allowed and the
     *                                   pointer at {@code idx} is 0
     * @throws IndexOutOfBoundsException if {@code idx} is out of range
     * @throws IllegalStateException     if a pointer to 0 is found in a non-nullable array
     */
    @SuppressWarnings("unchecked")
    protected <E> T getValue(int idx, Class<E> clazz, boolean nullsAllowed, jnr.ffi.Runtime r) {
        if (idx < 0 || idx >= items.length) {
            throw new IndexOutOfBoundsException(String.format("idx %d length %d", idx, len.intValue()));
        }
        // Null handling
        if (this.items[idx].address() == 0) {
            if (nullsAllowed) {
                return (T) null;
            } else {
                throw new IllegalStateException(String.format("Null found in non-nullable array at idx %d", idx));
            }
        }
        if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
            return (T) Integer.valueOf(this.items[idx].getInt(0));
        } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
            return (T) Long.valueOf(this.items[idx].getLong(0));
        } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
            return (T) Boolean.valueOf(this.items[idx].getByte(0) == 1);
        } else if (clazz.equals(String.class)) {
            // Read string length
            int length = this.items[idx].getInt(0);
            if (length < 0) {
                throw new IllegalStateException(String.format("Read string length of %d at index %d", length, idx));
            }
            // Decode the bytes as UTF-8
            byte[] utf8String = new byte[length];
            this.items[idx].get(4, utf8String, 0, length);
            return (T) new String(utf8String, StandardCharsets.UTF_8);
        } else if (clazz.equals(byte.class.arrayType())) {
            // Read the length of the byte array
            int length = this.items[idx].getInt(0);
            if (length < 0) {
                throw new IllegalStateException(String.format("Read byte[] length of %d at index %d", length, idx));
            }
            // Grab the actual bytes into the new array
            byte[] bytes = new byte[length];
            this.items[idx].get(4, bytes, 0, length);
            return (T) bytes;
        } else {
            throw new ClassCastException("Can't cast " + clazz + " to a valid Sleeper row key type");
        }
    }
}
