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
package sleeper.core.schema.type;

import java.util.Objects;

/**
 * For now, limit key and value to primitives.
 */
public class MapType implements Type {
    private final PrimitiveType keyType;
    private final PrimitiveType valueType;

    public MapType(PrimitiveType keyType, PrimitiveType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public PrimitiveType getKeyType() {
        return keyType;
    }

    public PrimitiveType getValueType() {
        return valueType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MapType mapType = (MapType) o;
        return keyType.equals(mapType.keyType) &&
                valueType.equals(mapType.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyType, valueType);
    }

    @Override
    public String toString() {
        return "MapType{" +
                "keyType=" + keyType +
                ", valueType=" + valueType +
                '}';
    }
}
