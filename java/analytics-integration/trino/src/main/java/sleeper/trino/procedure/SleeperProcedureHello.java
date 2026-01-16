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
package sleeper.trino.procedure;

import com.google.common.collect.ImmutableList;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;

public class SleeperProcedureHello {
    private static final MethodHandle HELLO_METHOD;

    static {
        try {
            HELLO_METHOD = lookup().unreflect(SleeperProcedureHello.class.getMethod("entryPoint", String.class));
        } catch (IllegalAccessException | NoSuchMethodException | SecurityException e) {
            throw new AssertionError(e);
        }
    }

    public static Procedure getProcedure() {
        return new Procedure(
                "runtime",
                "LOG_HELLO",
                ImmutableList.of(new Procedure.Argument("NAME", VARCHAR)),
                HELLO_METHOD.bindTo(new SleeperProcedureHello()));
    }

    public void entryPoint(String name) {
        System.out.printf("Hello, %s%n", name);
    }
}
