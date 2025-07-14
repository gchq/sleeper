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
package sleeper.core.row;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.core.key.Key;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Compares keys.
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_MIGHT_BE_INFEASIBLE")
public class KeyComparator implements Comparator<Key> {
    private final List<PrimitiveType> rowKeyTypes;

    public KeyComparator() {
        this.rowKeyTypes = new ArrayList<>();
    }

    public KeyComparator(List<PrimitiveType> rowKeyTypes) {
        this.rowKeyTypes = new ArrayList<>(rowKeyTypes);
    }

    public KeyComparator(PrimitiveType... rowKeyTypes) {
        this();
        for (PrimitiveType type : rowKeyTypes) {
            this.rowKeyTypes.add(type);
        }
    }

    @Override
    public int compare(Key key1, Key key2) {
        for (int i = 0; i < rowKeyTypes.size(); i++) {
            PrimitiveType type = rowKeyTypes.get(i);
            int diff = type.compare(key1.get(i), key2.get(i));
            if (0 != diff) {
                return diff;
            }
        }
        return 0;
    }
}
