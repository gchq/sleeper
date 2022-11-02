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
package sleeper.console;

import java.util.Objects;
import java.util.Optional;

public class Chosen<T extends ChooseOne.Choice> {

    private final T choice;
    private final boolean exited;

    public Chosen(T choice) {
        this(choice, false);
    }

    private Chosen(T choice, boolean exited) {
        this.choice = choice;
        this.exited = exited;
    }

    public Optional<T> getChoice() {
        return Optional.ofNullable(choice);
    }

    public boolean isExited() {
        return exited;
    }

    public static <T extends ChooseOne.Choice> Chosen<T> exit() {
        return new Chosen<>(null, true);
    }

    public static <T extends ChooseOne.Choice> Chosen<T> nothing() {
        return new Chosen<>(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Chosen<?> chosen = (Chosen<?>) o;
        return exited == chosen.exited && Objects.equals(choice, chosen.choice);
    }

    @Override
    public int hashCode() {
        return Objects.hash(choice, exited);
    }
}
