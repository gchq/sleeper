package sleeper.cdk.stack.bulkimport;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Locale;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

/**
 *
 */
public class AbstractBulkImportStack extends NestedStack {
    protected final InstanceProperties instanceProperties;
    protected IBucket importBucket;
    
    @SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
    public AbstractBulkImportStack(Construct scope,
            String id,
            InstanceProperties instanceProperties) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        createImportBucket();
    }

    protected void createImportBucket() {
        // NB This method will be called more than once if more than one bulk
        // import stack is deployed so we need to avoid creating the same bucket
        // multiple times.
        if (null != instanceProperties.get(BULK_IMPORT_BUCKET)) {
            importBucket = Bucket.fromBucketName(this, "BulkImportBucket", instanceProperties.get(BULK_IMPORT_BUCKET));
        } else {
            importBucket = Bucket.Builder.create(this, "BulkImportBucket")
                    .bucketName(String.join("-", "sleeper", instanceProperties.get(ID),
                            "bulk-import").toLowerCase(Locale.ROOT))
                    .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                    .versioned(false)
                    .autoDeleteObjects(true)
                    .removalPolicy(RemovalPolicy.DESTROY)
                    .encryption(BucketEncryption.S3_MANAGED)
                    .build();
            instanceProperties.set(BULK_IMPORT_BUCKET, importBucket.getBucketName());
        }
    }
}
