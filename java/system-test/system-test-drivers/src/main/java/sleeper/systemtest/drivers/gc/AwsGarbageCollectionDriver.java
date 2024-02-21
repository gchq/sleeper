package sleeper.systemtest.drivers.gc;

import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_LAMBDA_FUNCTION;

public class AwsGarbageCollectionDriver implements GarbageCollectionDriver {

    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;

    public AwsGarbageCollectionDriver(SystemTestInstanceContext instance, LambdaClient lambdaClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
    }

    @Override
    public void collectGarbage() {
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(GARBAGE_COLLECTOR_LAMBDA_FUNCTION));
    }

}
