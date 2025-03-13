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
 * A list whose elements are of a given type. The element must be a {@link PrimitiveType}.
 */
public class ListType implements Type {
    private final PrimitiveType elementType;

    public ListType(PrimitiveType elementType) {
        this.elementType = elementType;
    }

    public PrimitiveType getElementType() {
        return elementType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ListType listType = (ListType) o;
        return elementType.equals(listType.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementType);
    }

    @Override
    public String toString() {
        return "ListType{" +
                "elementType=" + elementType +
                '}';
    }
}
