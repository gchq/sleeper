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
package sleeper.core.range;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

public class RegionCanonicaliserTest {

    @Test
    public void shouldAnswerIsRegionInCanonicalFormCorrectly() {
       // Given
       Schema schema = new Schema();
       Field field1 = new Field("key1", new IntType());
       Field field2 = new Field("key2", new LongType());
       schema.setRowKeyFields(field1, field2);
       RangeFactory rangeFactory = new RangeFactory(schema);
       Range range1 = rangeFactory.createRange(field1, 1, 10);
       Range range2 = rangeFactory.createRange(field2, 100L, true, 200L, true);
       Region region1 = new Region(Arrays.asList(range1, range2));
       Range range3 = rangeFactory.createRange(field1, 1, 10);
       Range range4 = rangeFactory.createRange(field2, 100L, 1000L);
       Region region2 = new Region(Arrays.asList(range3, range4));
       
       // When / Then
       assertFalse(RegionCanonicaliser.isRegionInCanonicalForm(region1));
       assertTrue(RegionCanonicaliser.isRegionInCanonicalForm(region2));
    }
    
    @Test
    public void shouldCanonicaliseRegionCorrectly() {
       // Given
       Schema schema = new Schema();
       Field field1 = new Field("key1", new IntType());
       Field field2 = new Field("key2", new LongType());
       schema.setRowKeyFields(field1, field2);
       RangeFactory rangeFactory = new RangeFactory(schema);
       Range range1 = rangeFactory.createRange(field1, 1, 10);
       Range range2 = rangeFactory.createRange(field2, 100L, true, 200L, true);
       Region region1 = new Region(Arrays.asList(range1, range2));
       Range range3 = rangeFactory.createRange(field1, 1, 10);
       Range range4 = rangeFactory.createRange(field2, 100L, 1000L);
       Region region2 = new Region(Arrays.asList(range3, range4));
       
       // When
       Region canonicalisedRegion1 = RegionCanonicaliser.canonicaliseRegion(region1);
       Region canonicalisedRegion2 = RegionCanonicaliser.canonicaliseRegion(region2);
       
       // Then
       assertEquals(range1, canonicalisedRegion1.getRange("key1"));
       assertEquals(rangeFactory.createRange(field2, 100L, 201L), canonicalisedRegion1.getRange("key2"));
       assertEquals(range3, canonicalisedRegion2.getRange("key1"));
       assertEquals(range4, canonicalisedRegion2.getRange("key2"));
    }
}
