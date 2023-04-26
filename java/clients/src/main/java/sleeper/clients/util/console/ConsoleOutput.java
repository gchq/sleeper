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
package sleeper.clients.util.console;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class ConsoleOutput {

    public static final String CLEAR_CONSOLE = "\033[H\033[2J";

    private final PrintStream out;

    public ConsoleOutput(PrintStream out) {
        this.out = out;
    }

    public void clearScreen(String message) {
        out.print(CLEAR_CONSOLE);
        out.flush();
        out.println(message);
    }

    public void println(String message) {
        out.println(message);
    }

    public void println() {
        out.println();
    }

    public void printf(String format, Object... args) {
        out.printf(format, args);
    }

    public PrintWriter writer() {
        return new PrintWriter(out, true, StandardCharsets.UTF_8);
    }

    public PrintStream printStream() {
        return out;
    }
}
