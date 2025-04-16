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
package sleeper.core.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * This class is used to print out to the console.
 */
public class ConsoleOutput {

    public static final String CLEAR_CONSOLE = "\033[H\033[2J";

    private final PrintStream out;

    public ConsoleOutput(PrintStream out) {
        this.out = out;
    }

    /**
     * This method clears the console screen then prints the input on a new line.
     *
     * @param message String message to be printed after screen is cleared.
     */
    public void clearScreen(String message) {
        out.print(CLEAR_CONSOLE);
        out.flush();
        out.println(message);
    }

    /**
     * This method prints the input message on a new line.
     *
     * @param message String message to be printed on a new line.
     */
    public void println(String message) {
        out.println(message);
    }

    /**
     * This method prints a blank line.
     */
    public void println() {
        out.println();
    }

    /**
     * This method prints to the console using a formatter.
     *
     * @param format String format to be used for the message.
     * @param args   A number of args to be passed into the formatter.
     */
    public void printf(String format, Object... args) {
        out.printf(format, args);
    }

    /**
     * This method returns a new PrintWriter with the PrintStream of this object, autoFlush set to true and
     * StandardCharsets.UTF_8.
     *
     * @return A new PrintWriter with the PrintStream of this object, autoFlush set to true and
     *         StandardCharsets.UTF_8.
     */
    public PrintWriter writer() {
        return new PrintWriter(out, true, StandardCharsets.UTF_8);
    }

    /**
     * This method returns the PrintStream object assigned to the out variable.
     *
     * @return The PrintStream object assigned to the out variable.
     */
    public PrintStream printStream() {
        return out;
    }
}
