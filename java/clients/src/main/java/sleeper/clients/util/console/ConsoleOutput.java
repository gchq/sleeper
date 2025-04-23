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
package sleeper.clients.util.console;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * Writes to the console.
 */
public class ConsoleOutput {

    public static final String CLEAR_CONSOLE = "\033[H\033[2J";

    private final PrintStream out;

    public ConsoleOutput(PrintStream out) {
        this.out = out;
    }

    /**
     * Clears the console screen and prints a message followed by a line delimiter.
     *
     * @param message the message to print after the screen is cleared
     */
    public void clearScreen(String message) {
        out.print(CLEAR_CONSOLE);
        out.flush();
        out.println(message);
    }

    /**
     * Prints a message followed by a line delimiter.
     *
     * @param message the message
     */
    public void println(String message) {
        out.println(message);
    }

    /**
     * Prints a line delimiter and nothing else.
     */
    public void println() {
        out.println();
    }

    /**
     * Prints a message with a format string. Uses {@link java.util.Formatter}.
     *
     * @param format the format string
     * @param args   the arguments for the format string
     */
    public void printf(String format, Object... args) {
        out.printf(format, args);
    }

    /**
     * Creates a print writer to write to the console. Sets autoFlush to true. Uses {@link StandardCharsets#UTF_8}.
     *
     * @return the print writer
     */
    public PrintWriter writer() {
        return new PrintWriter(out, true, StandardCharsets.UTF_8);
    }

    /**
     * Returns the underlying print stream that writes to the console.
     *
     * @return the print stream
     */
    public PrintStream printStream() {
        return out;
    }
}
