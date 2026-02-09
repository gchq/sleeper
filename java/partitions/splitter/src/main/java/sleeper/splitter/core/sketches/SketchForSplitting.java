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
package sleeper.splitter.core.sketches;

/**
 * Finds split points during partition splitting based on a sketch. We avoid referencing the sketch directly so that we
 * can split a sketch into two while still reading the original sketch, when extending a partition tree over multiple
 * levels.
 */
public interface SketchForSplitting {

    /**
     * Retrieves the number of rows that were used to create the sketch.
     *
     * @return the number of rows
     */
    long getNumberOfRows();

    /**
     * Retrieves the minimum value in the range of values covered.
     *
     * @return the minimum value
     */
    Object getMin();

    /**
     * Retrieves the median value in the range of values covered.
     *
     * @return the median value
     */
    Object getMedian();

    /**
     * Retrieves the maximum value in the range of values covered.
     *
     * @return the maximum value
     */
    Object getMax();

}
