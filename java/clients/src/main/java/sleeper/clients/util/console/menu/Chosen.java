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
package sleeper.clients.util.console.menu;

import sleeper.clients.util.console.UserExitedException;

import java.util.Optional;
import java.util.function.Supplier;

public class Chosen<T extends ConsoleChoice> {

    private final String entered;
    private final T choice;

    public Chosen(String entered, T choice) {
        this.entered = entered;
        this.choice = choice;
    }

    public static <T extends ConsoleChoice> Chosen<T> nothing(String entered) {
        return new Chosen<>(entered, null);
    }

    public String getEntered() {
        return entered;
    }

    public Optional<T> getChoice() {
        return Optional.ofNullable(choice);
    }

    public T chooseUntilChoiceFound(Supplier<Chosen<T>> chooseAgain) throws UserExitedException {
        if (choice != null) {
            return choice;
        } else {
            return chooseAgain.get().chooseUntilChoiceFound(chooseAgain);
        }
    }

    public Chosen<T> chooseUntilSomethingEntered(Supplier<Chosen<T>> chooseAgain) throws UserExitedException {
        if ("".equals(entered)) {
            return chooseAgain.get().chooseUntilSomethingEntered(chooseAgain);
        } else {
            return this;
        }
    }
}
