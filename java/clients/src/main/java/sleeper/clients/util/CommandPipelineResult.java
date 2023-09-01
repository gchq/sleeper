/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.clients.util;

import java.util.Arrays;

public class CommandPipelineResult {

    private final int[] exitCodes;

    public CommandPipelineResult(int... exitCodes) {
        this.exitCodes = exitCodes;
    }

    public int[] getExitCodes() {
        return exitCodes;
    }

    public int getLastExitCode() {
        return exitCodes[exitCodes.length - 1];
    }

    @Override
    public String toString() {
        if (exitCodes.length == 1) {
            return String.valueOf(exitCodes[0]);
        } else {
            return Arrays.toString(exitCodes);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommandPipelineResult result = (CommandPipelineResult) o;
        return Arrays.equals(exitCodes, result.exitCodes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(exitCodes);
    }
}
