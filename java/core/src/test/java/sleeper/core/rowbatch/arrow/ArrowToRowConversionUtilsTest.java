package sleeper.core.rowbatch.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ArrowToRowConversionUtilsTest {

    private VectorSchemaRoot root;
    private Schema rootSchema;
    private FieldVector intVector;
    private FieldVector stringVector;
    private FieldVector listVector;
    private FieldVector mapVector;

    @BeforeEach
    void setup() {
        root = mock(VectorSchemaRoot.class);
        rootSchema = mock(Schema.class);
        when(root.getSchema()).thenReturn(rootSchema);
        intVector = mock(FieldVector.class);
        stringVector = mock(FieldVector.class);
        listVector = mock(FieldVector.class);
        mapVector = mock(FieldVector.class);
    }

    @AfterEach
    void teardown() {
        root = null;
        rootSchema = null;
        intVector = null;
        stringVector = null;
        listVector = null;
        mapVector = null;
    }

    @Test
    void shouldConvertPrimitiveFields() {
        // Given

        when(intVector.getName()).thenReturn("age");
        when(intVector.getMinorType()).thenReturn(Types.MinorType.INT);
        when(intVector.getObject(0)).thenReturn(42);
        when(stringVector.getName()).thenReturn("name");
        when(stringVector.getMinorType()).thenReturn(Types.MinorType.VARCHAR);
        when(stringVector.getObject(0)).thenReturn(new Text("Bob"));

        when(root.getSchema().getFields()).thenReturn(Arrays.asList(
                mock(Field.class),
                mock(Field.class)));
        when(root.getVector(0)).thenReturn(intVector);
        when(root.getVector(1)).thenReturn(stringVector);

        // When
        Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);
        // Then
        assertThat(row.get("age")).isEqualTo(42);
        assertThat(row.get("name")).isEqualTo("Bob");
    }

    @Test
    void shouldConvertTextToString() {
        // Given
        when(stringVector.getMinorType()).thenReturn(Types.MinorType.VARCHAR);
        Text text = new Text("hello world");

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(stringVector, text);

        // Then
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("hello world");
    }

    @Test
    void shouldConvertEmptyList() {
        // Given
        when(listVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(listVector.getChildrenFromFields()).thenReturn(List.of(mock(FieldVector.class)));

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(listVector, Collections.emptyList());

        // Then
        assertThat(result).isInstanceOf(List.class);
        assertThat((List<?>) result).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConvertListOfText() {
        // Given
        FieldVector childVector = mock(FieldVector.class);
        when(listVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(listVector.getChildrenFromFields()).thenReturn(List.of(childVector));
        List<Object> value = List.of(new Text("a"), new Text("b"));

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(listVector, value);

        // Then
        assertThat(result).isInstanceOf(List.class);
        List<Object> resultList = (List<Object>) result;
        assertThat(resultList).containsExactly("a", "b");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConvertDeeplyNestedList() {
        // Given
        FieldVector childVector = mock(FieldVector.class);
        when(listVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(listVector.getChildrenFromFields()).thenReturn(List.of(childVector));
        FieldVector grandChildVector = mock(FieldVector.class);
        when(childVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(childVector.getChildrenFromFields()).thenReturn(List.of(grandChildVector));
        List<Object> innerList = List.of(new Text("x"), new Text("y"));
        List<Object> value = List.of(innerList);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(listVector, value);

        // Then
        assertThat(result).isInstanceOf(List.class);
        List<Object> outer = (List<Object>) result;
        assertThat(outer.get(0)).isInstanceOf(List.class);
        assertThat((List<Object>) outer.get(0)).containsExactly("x", "y");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConvertMap() {
        // Given
        FieldVector structVector = mock(FieldVector.class);
        FieldVector keyVector = mock(FieldVector.class);
        FieldVector valueVector = mock(FieldVector.class);

        when(mapVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(mapVector.getChildrenFromFields()).thenReturn(List.of(structVector));
        when(structVector.getMinorType()).thenReturn(Types.MinorType.STRUCT);
        when(structVector.getChildrenFromFields()).thenReturn(List.of(keyVector, valueVector));
        when(keyVector.getField()).thenReturn(mock(Field.class));
        when(keyVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_KEY_FIELD_NAME);
        when(valueVector.getField()).thenReturn(mock(Field.class));
        when(valueVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_VALUE_FIELD_NAME);

        Map<String, Object> entry1 = new HashMap<>();
        entry1.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("fruit"));
        entry1.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, new Text("apple"));

        Map<String, Object> entry2 = new HashMap<>();
        entry2.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("color"));
        entry2.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, new Text("red"));

        List<Map<String, Object>> arrowMapValue = List.of(entry1, entry2);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(mapVector, arrowMapValue);

        // Then
        assertThat(result).isInstanceOf(Map.class);
        Map<Object, Object> resultMap = (Map<Object, Object>) result;
        assertThat(resultMap).containsEntry("fruit", "apple");
        assertThat(resultMap).containsEntry("color", "red");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConvertEmptyMap() {
        // Given
        FieldVector structVector = mock(FieldVector.class);
        FieldVector keyVector = mock(FieldVector.class);
        FieldVector valueVector = mock(FieldVector.class);

        when(mapVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(mapVector.getChildrenFromFields()).thenReturn(List.of(structVector));
        when(structVector.getMinorType()).thenReturn(Types.MinorType.STRUCT);
        when(structVector.getChildrenFromFields()).thenReturn(List.of(keyVector, valueVector));
        when(keyVector.getField()).thenReturn(mock(Field.class));
        when(keyVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_KEY_FIELD_NAME);
        when(valueVector.getField()).thenReturn(mock(Field.class));
        when(valueVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_VALUE_FIELD_NAME);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(mapVector, Collections.emptyList());

        // Then
        assertThat(result).isInstanceOf(Map.class);
        assertThat((Map<Object, Object>) result).isEmpty();
    }

    @Test
    void shouldConvertValueWithNull() {
        // Given
        when(stringVector.getMinorType()).thenReturn(Types.MinorType.VARCHAR);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(stringVector, null);

        // Then
        assertThat(result).isNull();
    }

    @Test
    void shouldConvertUnknownTypeReturnsValue() {
        // Given
        when(intVector.getMinorType()).thenReturn(Types.MinorType.INT);
        Object value = 100;

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(intVector, value);

        // Then
        assertThat(result).isEqualTo(100);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNestedMapAsListOfStructs() {
        // Given
        FieldVector structVector = mock(FieldVector.class);
        FieldVector keyVector = mock(FieldVector.class);
        FieldVector valueVector = mock(FieldVector.class);

        when(listVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(listVector.getChildrenFromFields()).thenReturn(List.of(structVector));
        when(structVector.getMinorType()).thenReturn(Types.MinorType.STRUCT);
        when(structVector.getChildrenFromFields()).thenReturn(List.of(keyVector, valueVector));
        when(keyVector.getField()).thenReturn(mock(Field.class));
        when(keyVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_KEY_FIELD_NAME);
        when(valueVector.getField()).thenReturn(mock(Field.class));
        when(valueVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_VALUE_FIELD_NAME);

        Map<String, Object> entry = new HashMap<>();
        entry.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("number"));
        entry.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, 88);

        List<Map<String, Object>> arrowMapValue = Collections.singletonList(entry);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(listVector, arrowMapValue);

        // Then
        assertThat(result).isInstanceOf(Map.class);
        Map<Object, Object> resultMap = (Map<Object, Object>) result;
        assertThat(resultMap).containsEntry("number", 88);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldListWithNullsAndNestedLists() {
        // Given

        FieldVector childVector = mock(FieldVector.class);
        when(listVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(listVector.getChildrenFromFields()).thenReturn(List.of(childVector));
        FieldVector grandChildVector = mock(FieldVector.class);
        when(childVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(childVector.getChildrenFromFields()).thenReturn(List.of(grandChildVector));

        List<Object> value = Arrays.asList(null, Arrays.asList(new Text("z")));

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(listVector, value);

        // Then
        assertThat(result).isInstanceOf(List.class);
        List<Object> resultList = (List<Object>) result;
        assertThat(resultList.get(0)).isNull();
        assertThat(resultList.get(1)).isInstanceOf(List.class);
        assertThat(((List<?>) resultList.get(1)).get(0)).isInstanceOf(String.class);
        assertThat((List<String>) resultList.get(1)).containsExactly("z");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMapWithNullKeys() {
        // Given
        FieldVector structVector = mock(FieldVector.class);
        FieldVector keyVector = mock(FieldVector.class);
        FieldVector valueVector = mock(FieldVector.class);

        when(mapVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(mapVector.getChildrenFromFields()).thenReturn(List.of(structVector));
        when(structVector.getMinorType()).thenReturn(Types.MinorType.STRUCT);
        when(structVector.getChildrenFromFields()).thenReturn(List.of(keyVector, valueVector));
        when(keyVector.getField()).thenReturn(mock(Field.class));
        when(keyVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_KEY_FIELD_NAME);
        when(valueVector.getField()).thenReturn(mock(Field.class));
        when(valueVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_VALUE_FIELD_NAME);

        Map<String, Object> entry1 = new HashMap<>();
        entry1.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, null);
        entry1.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, new Text("something"));

        Map<String, Object> entry2 = new HashMap<>();
        entry2.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("other"));
        entry2.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, new Text("thing"));

        List<Map<String, Object>> arrowMapValue = Arrays.asList(entry1, entry2);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(mapVector, arrowMapValue);

        // Then
        assertThat(result).isInstanceOf(Map.class);
        Map<Object, Object> resultMap = (Map<Object, Object>) result;
        assertThat(resultMap).containsEntry(null, "something");
        assertThat(resultMap).containsEntry("other", "thing");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConvertNestedMapsConversion() {
        // Given
        FieldVector structVector = mock(FieldVector.class);
        FieldVector keyVector = mock(FieldVector.class);
        FieldVector valueVector = mock(FieldVector.class);
        FieldVector nestedStructVector = mock(FieldVector.class);

        // Set up top-level map (LIST of STRUCT)
        when(mapVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(mapVector.getChildrenFromFields()).thenReturn(List.of(structVector));
        when(structVector.getMinorType()).thenReturn(Types.MinorType.STRUCT);
        when(structVector.getChildrenFromFields()).thenReturn(List.of(keyVector, valueVector));
        when(keyVector.getField()).thenReturn(mock(Field.class));
        when(keyVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_KEY_FIELD_NAME);
        when(valueVector.getField()).thenReturn(mock(Field.class));
        when(valueVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_VALUE_FIELD_NAME);

        // Set up nested map field
        when(valueVector.getMinorType()).thenReturn(Types.MinorType.LIST);
        when(valueVector.getChildrenFromFields()).thenReturn(List.of(nestedStructVector));
        when(nestedStructVector.getMinorType()).thenReturn(Types.MinorType.STRUCT);
        FieldVector nestedKeyVector = mock(FieldVector.class);
        FieldVector nestedValueVector = mock(FieldVector.class);
        when(nestedStructVector.getChildrenFromFields()).thenReturn(List.of(nestedKeyVector, nestedValueVector));
        when(nestedKeyVector.getField()).thenReturn(mock(Field.class));
        when(nestedKeyVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_KEY_FIELD_NAME);
        when(nestedValueVector.getField()).thenReturn(mock(Field.class));
        when(nestedValueVector.getField().getName()).thenReturn(ArrowRowBatch.MAP_VALUE_FIELD_NAME);

        // Compose nested map data
        Map<String, Object> nestedMapEntry1 = new HashMap<>();
        nestedMapEntry1.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("inner1"));
        nestedMapEntry1.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, new Text("value1"));

        Map<String, Object> nestedMapEntry2 = new HashMap<>();
        nestedMapEntry2.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("inner2"));
        nestedMapEntry2.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, new Text("value2"));

        List<Map<String, Object>> nestedMapList = List.of(nestedMapEntry1, nestedMapEntry2);

        // Compose top-level map data
        Map<String, Object> topEntry = new HashMap<>();
        topEntry.put(ArrowRowBatch.MAP_KEY_FIELD_NAME, new Text("parent"));
        topEntry.put(ArrowRowBatch.MAP_VALUE_FIELD_NAME, nestedMapList);

        List<Map<String, Object>> topMapList = List.of(topEntry);

        // When
        Object result = ArrowToRowConversionUtils.convertValueFromArrow(mapVector, topMapList);

        // Then
        assertThat(result).isInstanceOf(Map.class);
        Map<Object, Object> parentMap = (Map<Object, Object>) result;
        Object nestedObject = parentMap.get("parent");
        assertThat(nestedObject).isInstanceOf(Map.class);
        Map<Object, Object> innerMap = (Map<Object, Object>) nestedObject;
        assertThat(innerMap).containsEntry("inner1", "value1");
        assertThat(innerMap).containsEntry("inner2", "value2");
    }
}