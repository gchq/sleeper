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
package sleeper.core.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

public class FieldTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Field field1 = new Field("key", new IntType());
        Field field2 = new Field("key", new IntType());
        Field field3 = new Field("key", new LongType());
        Field field4 = new Field("key2", new LongType());
        
        // When
        boolean equals1 = field1.equals(field2);
        boolean equals2 = field1.equals(field3);
        boolean equals3 = field3.equals(field4);
        int hashCode1 = field1.hashCode();
        int hashCode2 = field2.hashCode();
        int hashCode3 = field3.hashCode();
        int hashCode4 = field4.hashCode();
        
        // Then
        assertTrue(equals1);
        assertFalse(equals2);
        assertFalse(equals3);
        assertEquals(hashCode1, hashCode2);
        assertNotEquals(hashCode1, hashCode3);
        assertNotEquals(hashCode3, hashCode4);
    }
}
