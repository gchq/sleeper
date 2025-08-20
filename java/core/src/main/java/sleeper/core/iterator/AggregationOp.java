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
package sleeper.core.iterator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

/** Aggregation functions that can be performed on values. */
public enum AggregationOp implements BinaryOperator<Object> {
    SUM {
        @Override
        public int op(int lhs, int rhs) {
            return lhs + rhs;
        }

        @Override
        public long op(long lhs, long rhs) {
            return lhs + rhs;
        }

        @Override
        public String op(String lhs, String rhs) {
            return lhs + rhs;
        }

        @Override
        public byte[] op(byte[] lhs, byte[] rhs) {
            byte[] concatenated = Arrays.copyOf(lhs, lhs.length + rhs.length);
            System.arraycopy(rhs, 0, concatenated, lhs.length, rhs.length);
            return concatenated;
        }
    },
    MIN {
        @Override
        public int op(int lhs, int rhs) {
            return Math.min(lhs, rhs);
        }

        @Override
        public long op(long lhs, long rhs) {
            return Math.min(lhs, rhs);
        }

        @Override
        public String op(String lhs, String rhs) {
            if (lhs.compareTo(rhs) <= 0) {
                return lhs;
            } else {
                return rhs;
            }
        }

        @Override
        public byte[] op(byte[] lhs, byte[] rhs) {
            if (Arrays.compareUnsigned(lhs, rhs) <= 0) {
                return lhs;
            } else {
                return rhs;
            }
        }
    },
    MAX {
        @Override
        public int op(int lhs, int rhs) {
            return Math.max(lhs, rhs);
        }

        @Override
        public long op(long lhs, long rhs) {
            return Math.max(lhs, rhs);
        }

        @Override
        public String op(String lhs, String rhs) {
            if (lhs.compareTo(rhs) > 0) {
                return lhs;
            } else {
                return rhs;
            }
        }

        @Override
        public byte[] op(byte[] lhs, byte[] rhs) {
            if (Arrays.compareUnsigned(lhs, rhs) > 0) {
                return lhs;
            } else {
                return rhs;
            }
        }
    };

    /**
     * Perform aggregation operation on integer arguments.
     *
     * @param  lhs left hand operand
     * @param  rhs right hand operand
     * @return     combined value
     */
    public abstract int op(int lhs, int rhs);

    /**
     * Perform aggregation operation on long arguments.
     *
     * @param  lhs left hand operand
     * @param  rhs right hand operand
     * @return     combined value
     */
    public abstract long op(long lhs, long rhs);

    /**
     * Perform aggregation operation on String arguments.
     *
     * @param  lhs left hand operand
     * @param  rhs right hand operand
     * @return     combined value
     */
    public abstract String op(String lhs, String rhs);

    /**
     * Perform aggregation operation on byte array arguments.
     *
     * @param  lhs left hand operand
     * @param  rhs right hand operand
     * @return     combined value
     */
    public abstract byte[] op(byte[] lhs, byte[] rhs);

    @Override
    public Object apply(Object lhs, Object rhs) {
        if (!(lhs instanceof Map && rhs instanceof Map) && lhs.getClass() != rhs.getClass()) {
            throw new IllegalArgumentException("different operands, lhs type: " + lhs.getClass() + " rhs type: " + rhs.getClass());
        }
        if (lhs instanceof Integer) {
            return op((Integer) lhs, (Integer) rhs);
        } else if (lhs instanceof Long) {
            return op((Long) lhs, (Long) rhs);
        } else if (lhs instanceof String) {
            return op((String) lhs, (String) rhs);
        } else if (lhs instanceof byte[]) {
            return op((byte[]) lhs, (byte[]) rhs);
        } else if (lhs instanceof Map) {
            // Clone the map to avoid aliasing problems: don't want to alter map in previous row
            Map<?, ?> mapLhs = new HashMap<>((Map<?, ?>) lhs);
            // Find first value of RHS if there is one
            Object testValue = ((Map<?, ?>) rhs).values().stream().findFirst().orElse(null);
            if (testValue != null) {
                // Loop over right hand side entry set, determine value class and then delegate to primitive operations.
                if (testValue instanceof Integer) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Integer> castLhs = (Map<Object, Integer>) mapLhs;
                    @SuppressWarnings("unchecked")
                    Map<Object, Integer> castRhs = (Map<Object, Integer>) rhs;
                    for (Map.Entry<Object, Integer> entry : castRhs.entrySet()) {
                        castLhs.merge(entry.getKey(), entry.getValue(), (lhs_value, rhs_value) -> {
                            return op(lhs_value, rhs_value);
                        });
                    }
                } else if (testValue instanceof Long) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Long> castLhs = (Map<Object, Long>) mapLhs;
                    @SuppressWarnings("unchecked")
                    Map<Object, Long> castRhs = (Map<Object, Long>) rhs;
                    for (Map.Entry<Object, Long> entry : castRhs.entrySet()) {
                        castLhs.merge(entry.getKey(), entry.getValue(), (lhs_value, rhs_value) -> {
                            return op(lhs_value, rhs_value);
                        });
                    }
                } else if (testValue instanceof String) {
                    @SuppressWarnings("unchecked")
                    Map<Object, String> castLhs = (Map<Object, String>) mapLhs;
                    @SuppressWarnings("unchecked")
                    Map<Object, String> castRhs = (Map<Object, String>) rhs;
                    for (Map.Entry<Object, String> entry : castRhs.entrySet()) {
                        castLhs.merge(entry.getKey(), entry.getValue(), (lhs_value, rhs_value) -> {
                            return op(lhs_value, rhs_value);
                        });
                    }
                } else if (testValue instanceof byte[]) {
                    @SuppressWarnings("unchecked")
                    Map<Object, byte[]> castLhs = (Map<Object, byte[]>) mapLhs;
                    @SuppressWarnings("unchecked")
                    Map<Object, byte[]> castRhs = (Map<Object, byte[]>) rhs;
                    for (Map.Entry<Object, byte[]> entry : castRhs.entrySet()) {
                        castLhs.merge(entry.getKey(), entry.getValue(), (lhs_value, rhs_value) -> {
                            return op(lhs_value, rhs_value);
                        });
                    }
                } else {
                    throw new IllegalArgumentException("Value type not implemented " + testValue.getClass());
                }
            }
            return mapLhs;
        } else {
            throw new IllegalArgumentException("Value type not implemented " + lhs.getClass());
        }
    }
}
