package sleeper.trino.procedure;

import com.google.common.collect.ImmutableList;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class SleeperProcedureHello {
    private static final MethodHandle HELLO_METHOD = methodHandle(SleeperProcedureHello.class, "entryPoint", String.class);

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
