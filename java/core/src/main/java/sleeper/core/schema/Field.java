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

import sleeper.core.schema.type.Type;

import java.util.Objects;

/**
 * Describes a field in the {@link Schema}. It contains a {@link String} name
 * and a {@link Type}.
 */
public class Field {
    private String name;
    private Type type;
    
    public Field() {
    }
    
    public Field(String name, Type type) {
        setName(name);
        setType(type);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        Objects.requireNonNull(name, "name must not be null");
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        Objects.requireNonNull(type, "type must not be null");
        this.type = type;
    }

    @Override
    public String toString() {
        return "Field{" + "name=" + name + ", type=" + type + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 31 * hash + Objects.hashCode(this.name);
        hash = 31 * hash + Objects.hashCode(this.type);
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
        final Field other = (Field) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.type, other.type)) {
            return false;
        }
        return true;
    }
}
