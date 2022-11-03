package sleeper.compaction.jobexecution;

import com.amazonaws.services.ecs.AmazonECS;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Details about EC2 instances in an ECS cluster.
 */
public class InstanceDetails {
    /** The container instance ARN. */
    public final String instance_arn;
    /** When was the instance registered with the cluster. */
    public final Instant registered;
    /** Amount of RAM available for container use. */
    public final int availableCPU;
    /** Amount of CPU available for container use. */
    public final int availableRAM;
    /** Amount of CPU in total. */
    public final int totalCPU;
    /** Amount of RAM in total. */
    public final int totalRAM;

    public InstanceDetails(String instance_arn, Instant registered, int availableCPU, int availableRAM, int totalCPU,
            int totalRAM) {
        super();
        this.instance_arn = instance_arn;
        this.registered = registered;
        this.availableCPU = availableCPU;
        this.availableRAM = availableRAM;
        this.totalCPU = totalCPU;
        this.totalRAM = totalRAM;
    }

    @Override
    public int hashCode() {
        return Objects.hash(availableCPU, availableRAM, instance_arn, registered, totalCPU, totalRAM);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InstanceDetails other = (InstanceDetails) obj;
        return availableCPU == other.availableCPU && availableRAM == other.availableRAM
                && Objects.equals(instance_arn, other.instance_arn) && Objects.equals(registered, other.registered)
                && totalCPU == other.totalCPU && totalRAM == other.totalRAM;
    }

    @Override
    public String toString() {
        return "InstanceDetails [instance_arn=" + instance_arn + ", registered=" + registered + ", availableCPU="
                + availableCPU + ", availableRAM=" + availableRAM + ", totalCPU=" + totalCPU + ", totalRAM=" + totalRAM
                + "]";
    }
    
    public static Map<String,InstanceDetails> fetchInstanceDetails(String ecs_cluster_name, AmazonECS ecsClient) {
        Map<String,InstanceDetails> details= new HashMap<>();
        // Loop over the container instances in page size of 100
        
        return details;
    }

}
