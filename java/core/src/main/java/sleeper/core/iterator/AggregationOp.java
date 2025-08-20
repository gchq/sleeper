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
        public int op(int leftHandSide, int rightHandSide) {
            return leftHandSide + rightHandSide;
        }

        @Override
        public long op(long leftHandSide, long rightHandSide) {
            return leftHandSide + rightHandSide;
        }

        @Override
        public String op(String leftHandSide, String rightHandSide) {
            return leftHandSide + rightHandSide;
        }

        @Override
        public byte[] op(byte[] leftHandSide, byte[] rightHandSide) {
            byte[] concatenated = Arrays.copyOf(leftHandSide, leftHandSide.length + rightHandSide.length);
            System.arraycopy(rightHandSide, 0, concatenated, leftHandSide.length, rightHandSide.length);
            return concatenated;
        }
    },
    MIN {
        @Override
        public int op(int leftHandSide, int rightHandSide) {
            return Math.min(leftHandSide, rightHandSide);
        }

        @Override
        public long op(long leftHandSide, long rightHandSide) {
            return Math.min(leftHandSide, rightHandSide);
        }

        @Override
        public String op(String leftHandSide, String rightHandSide) {
            if (leftHandSide.compareTo(rightHandSide) <= 0) {
                return leftHandSide;
            } else {
                return rightHandSide;
            }
        }

        @Override
        public byte[] op(byte[] leftHandSide, byte[] rightHandSide) {
            if (Arrays.compareUnsigned(leftHandSide, rightHandSide) <= 0) {
                return leftHandSide;
            } else {
                return rightHandSide;
            }
        }
    },
    MAX {
        @Override
        public int op(int leftHandSide, int rightHandSide) {
            return Math.max(leftHandSide, rightHandSide);
        }

        @Override
        public long op(long leftHandSide, long rightHandSide) {
            return Math.max(leftHandSide, rightHandSide);
        }

        @Override
        public String op(String leftHandSide, String rightHandSide) {
            if (leftHandSide.compareTo(rightHandSide) > 0) {
                return leftHandSide;
            } else {
                return rightHandSide;
            }
        }

        @Override
        public byte[] op(byte[] leftHandSide, byte[] rightHandSide) {
            if (Arrays.compareUnsigned(leftHandSide, rightHandSide) > 0) {
                return leftHandSide;
            } else {
                return rightHandSide;
            }
        }
    };

    /**
     * Perform aggregation operation on integer arguments.
     *
     * @param  leftHandSide  left hand operand
     * @param  rightHandSide right hand operand
     * @return               combined value
     */
    public abstract int op(int leftHandSide, int rightHandSide);

    /**
     * Perform aggregation operation on long arguments.
     *
     * @param  leftHandSide  left hand operand
     * @param  rightHandSide right hand operand
     * @return               combined value
     */
    public abstract long op(long leftHandSide, long rightHandSide);

    /**
     * Perform aggregation operation on String arguments.
     *
     * @param  leftHandSide  left hand operand
     * @param  rightHandSide right hand operand
     * @return               combined value
     */
    public abstract String op(String leftHandSide, String rightHandSide);

    /**
     * Perform aggregation operation on byte array arguments.
     *
     * @param  leftHandSide  left hand operand
     * @param  rightHandSide right hand operand
     * @return               combined value
     */
    public abstract byte[] op(byte[] leftHandSide, byte[] rightHandSide);

    @Override
    public Object apply(Object leftHandSide, Object rightHandSide) {
        if (!(leftHandSide instanceof Map && rightHandSide instanceof Map) && leftHandSide.getClass() != rightHandSide.getClass()) {
            throw new IllegalArgumentException("different operands, leftHandSide type: " + leftHandSide.getClass() + " rightHandSide type: " + rightHandSide.getClass());
        }
        if (leftHandSide instanceof Integer) {
            return op((Integer) leftHandSide, (Integer) rightHandSide);
        } else if (leftHandSide instanceof Long) {
            return op((Long) leftHandSide, (Long) rightHandSide);
        } else if (leftHandSide instanceof String) {
            return op((String) leftHandSide, (String) rightHandSide);
        } else if (leftHandSide instanceof byte[]) {
            return op((byte[]) leftHandSide, (byte[]) rightHandSide);
        } else if (leftHandSide instanceof Map) {
            return handleMap((Map<?, ?>) leftHandSide, rightHandSide);
        } else {
            throw new IllegalArgumentException("Value type not implemented " + leftHandSide.getClass());
        }
    }

    private Map<?, ?> handleMap(Map<?, ?> leftHandSide, Object rightHandSide) {
        // Clone the map to avoid aliasing problems: don't want to alter map in previous row
        Map<?, ?> mapLeftHandSide = new HashMap<>(leftHandSide);
        // Find first value of RHS if there is one
        Object testValue = ((Map<?, ?>) rightHandSide).values().stream().findFirst().orElse(null);
        if (testValue != null) {
            // Loop over right hand side entry set, determine value class and then delegate to primitive operations.
            if (testValue instanceof Integer) {
                @SuppressWarnings("unchecked")
                Map<Object, Integer> castLeftHandSide = (Map<Object, Integer>) mapLeftHandSide;
                @SuppressWarnings("unchecked")
                Map<Object, Integer> castRightHandSide = (Map<Object, Integer>) rightHandSide;
                for (Map.Entry<Object, Integer> entry : castRightHandSide.entrySet()) {
                    castLeftHandSide.merge(entry.getKey(), entry.getValue(), (leftHandSide_value, rightHandSide_value) -> {
                        return op(leftHandSide_value, rightHandSide_value);
                    });
                }
            } else if (testValue instanceof Long) {
                @SuppressWarnings("unchecked")
                Map<Object, Long> castLeftHandSide = (Map<Object, Long>) mapLeftHandSide;
                @SuppressWarnings("unchecked")
                Map<Object, Long> castRightHandSide = (Map<Object, Long>) rightHandSide;
                for (Map.Entry<Object, Long> entry : castRightHandSide.entrySet()) {
                    castLeftHandSide.merge(entry.getKey(), entry.getValue(), (leftHandSide_value, rightHandSide_value) -> {
                        return op(leftHandSide_value, rightHandSide_value);
                    });
                }
            } else if (testValue instanceof String) {
                @SuppressWarnings("unchecked")
                Map<Object, String> castLeftHandSide = (Map<Object, String>) mapLeftHandSide;
                @SuppressWarnings("unchecked")
                Map<Object, String> castRightHandSide = (Map<Object, String>) rightHandSide;
                for (Map.Entry<Object, String> entry : castRightHandSide.entrySet()) {
                    castLeftHandSide.merge(entry.getKey(), entry.getValue(), (leftHandSide_value, rightHandSide_value) -> {
                        return op(leftHandSide_value, rightHandSide_value);
                    });
                }
            } else if (testValue instanceof byte[]) {
                @SuppressWarnings("unchecked")
                Map<Object, byte[]> castLeftHandSide = (Map<Object, byte[]>) mapLeftHandSide;
                @SuppressWarnings("unchecked")
                Map<Object, byte[]> castRightHandSide = (Map<Object, byte[]>) rightHandSide;
                for (Map.Entry<Object, byte[]> entry : castRightHandSide.entrySet()) {
                    castLeftHandSide.merge(entry.getKey(), entry.getValue(), (leftHandSide_value, rightHandSide_value) -> {
                        return op(leftHandSide_value, rightHandSide_value);
                    });
                }
            } else {
                throw new IllegalArgumentException("Value type not implemented " + testValue.getClass());
            }
        }
        return mapLeftHandSide;
    }
}
