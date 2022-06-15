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
package sleeper.core.key;

import org.junit.Test;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class KeySerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseCorrectly() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("rowkey1", new IntType()),
                new Field("rowkey2", new LongType()),
                new Field("rowkey3", new StringType()),
                new Field("rowkey4", new ByteArrayType()));
        KeySerDe keySerDe = new KeySerDe(schema);
        List<Object> keys = new ArrayList<>();
        keys.add(1);
        keys.add(10L);
        keys.add("abc");
        keys.add(new byte[]{1, 2, 3});
        Key key = Key.create(keys);
        
        // When
        byte[] serialised = keySerDe.serialise(key);
        Key deserialisedKey = keySerDe.deserialise(serialised);
        
        // Then
        assertEquals(4, deserialisedKey.size());
        assertEquals(1, deserialisedKey.get(0));
        assertEquals(10L, deserialisedKey.get(1));
        assertEquals("abc", deserialisedKey.get(2));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) deserialisedKey.get(3));
    }
    
    @Test
    public void shouldSerialiseAndDeserialiseNullIntKeyCorrectly() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("rowkey1", new IntType()));
        KeySerDe keySerDe = new KeySerDe(schema);
        Key key = Key.create(null);
        
        // When
        byte[] serialised = keySerDe.serialise(key);
        Key deserialisedKey = keySerDe.deserialise(serialised);
        
        // Then
        assertEquals(1, deserialisedKey.size());
        assertEquals(null, deserialisedKey.get(0));
    }
    
    @Test
    public void shouldSerialiseAndDeserialiseNullLongKeyCorrectly() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("rowkey1", new LongType()));
        KeySerDe keySerDe = new KeySerDe(schema);
        Key key = Key.create(null);
        
        // When
        byte[] serialised = keySerDe.serialise(key);
        Key deserialisedKeys = keySerDe.deserialise(serialised);
        
        // Then
        assertEquals(1, deserialisedKeys.size());
        assertEquals(null, deserialisedKeys.get(0));
    }
    
    @Test
    public void shouldSerialiseAndDeserialiseNullStringKeyCorrectly() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("rowkey1", new StringType()));
        KeySerDe keySerDe = new KeySerDe(schema);
        Key key = Key.create(null);
        
        // When
        byte[] serialised = keySerDe.serialise(key);
        Key deserialisedKeys = keySerDe.deserialise(serialised);
        
        // Then
        assertEquals(1, deserialisedKeys.size());
        assertEquals(null, deserialisedKeys.get(0));
    }
    
    @Test
    public void shouldSerialiseAndDeserialiseNullByteArrayKeyCorrectly() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("rowkey1", new ByteArrayType()));
        KeySerDe keySerDe = new KeySerDe(schema);
        Key key = Key.create(null);
        
        // When
        byte[] serialised = keySerDe.serialise(key);
        Key deserialisedKeys = keySerDe.deserialise(serialised);
        
        // Then
        assertEquals(1, deserialisedKeys.size());
        assertEquals(null, deserialisedKeys.get(0));
    }
    
    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyWhenThereAreFewerKeysThanInSchema() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("rowkey1", new IntType()),
                new Field("rowkey2", new LongType()),
                new Field("rowkey3", new StringType()),
                new Field("rowkey4", new ByteArrayType()));
        KeySerDe keySerDe = new KeySerDe(schema);
        List<Object> keys1 = new ArrayList<>();
        keys1.add(1);
        keys1.add(10L);
        keys1.add("abc");
        keys1.add(new byte[]{1, 2, 3});
        Key key1 = Key.create(keys1);
        List<Object> keys2 = new ArrayList<>();
        keys2.add(1);
        keys2.add(10L);
        keys2.add("abc");
        Key key2 = Key.create(keys2);
        List<Object> keys3 = new ArrayList<>();
        keys3.add(1);
        keys3.add(10L);
        Key key3 = Key.create(keys3);
        List<Object> keys4 = new ArrayList<>();
        keys4.add(1);
        Key key4 = Key.create(keys4);
        
        // When
        byte[] serialised1 = keySerDe.serialise(key1);
        Key deserialisedKey1 = keySerDe.deserialise(serialised1);
        byte[] serialised2 = keySerDe.serialise(key2);
        Key deserialisedKey2 = keySerDe.deserialise(serialised2);
        byte[] serialised3 = keySerDe.serialise(key3);
        Key deserialisedKey3 = keySerDe.deserialise(serialised3);
        byte[] serialised4 = keySerDe.serialise(key4);
        Key deserialisedKey4 = keySerDe.deserialise(serialised4);
        
        // Then
        assertEquals(4, deserialisedKey1.size());
        assertEquals(1, deserialisedKey1.get(0));
        assertEquals(10L, deserialisedKey1.get(1));
        assertEquals("abc", deserialisedKey1.get(2));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) deserialisedKey1.get(3));
        assertEquals(3, deserialisedKey2.size());
        assertEquals(1, deserialisedKey2.get(0));
        assertEquals(10L, deserialisedKey2.get(1));
        assertEquals("abc", deserialisedKey1.get(2));
        assertEquals(2, deserialisedKey3.size());
        assertEquals(1, deserialisedKey3.get(0));
        assertEquals(10L, deserialisedKey2.get(1));
        assertEquals(1, deserialisedKey4.size());
        assertEquals(1, deserialisedKey4.get(0));
    }
}
