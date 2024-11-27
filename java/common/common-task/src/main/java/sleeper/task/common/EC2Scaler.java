/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.task.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup;
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsResponse;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;

import sleeper.core.properties.instance.InstanceProperties;

/**
 * ECS EC2 auto scaler. This makes decisions on how many instances to start and stop based on the
 * amount of work there is to do.
 */
public class EC2Scaler {

    private static final Logger LOGGER = LoggerFactory.getLogger(EC2Scaler.class);

    private EC2Scaler() {
    }

    public static CompactionTaskHostScaler create(InstanceProperties instanceProperties, AutoScalingClient asClient, Ec2Client ec2Client) {
        return new CompactionTaskHostScaler(instanceProperties,
                autoScalingGroup -> getAutoScalingGroupMaxSize(autoScalingGroup, asClient),
                (autoScalingGroup, desiredSize) -> setClusterDesiredSize(asClient, autoScalingGroup, desiredSize),
                instanceType -> getEc2InstanceType(ec2Client, instanceType));
    }

    /**
     * Find the details of a given EC2 auto scaling group.
     *
     * @param  groupName the name of the auto scaling group
     * @param  client    the client object
     * @return           group data
     */
    private static int getAutoScalingGroupMaxSize(String groupName, AutoScalingClient client) {
        DescribeAutoScalingGroupsResponse result = client.describeAutoScalingGroups(req -> req.autoScalingGroupNames(groupName).maxRecords(1));
        if (result.autoScalingGroups().size() != 1) {
            throw new IllegalStateException("instead of 1, received " + result.autoScalingGroups().size()
                    + " records for describe_auto_scaling_groups on group name " + groupName);
        }
        AutoScalingGroup group = result.autoScalingGroups().get(0);
        LOGGER.debug("Auto scaling group instance count: minimum {}, desired size {}, maximum size {}",
                group.minSize(), group.desiredCapacity(), group.maxSize());
        return group.maxSize();
    }

    private static CompactionTaskHostScaler.InstanceType getEc2InstanceType(Ec2Client ec2Client, String instanceType) {
        DescribeInstanceTypesResponse result = ec2Client.describeInstanceTypes(req -> req.instanceTypesWithStrings(instanceType));
        if (result.instanceTypes().size() != 1) {
            throw new IllegalStateException("got more than 1 result for DescribeInstanceTypes for type " + instanceType);
        }
        InstanceTypeInfo typeInfo = result.instanceTypes().get(0);
        LOGGER.info("EC2 instance type info: {}", typeInfo);
        return new CompactionTaskHostScaler.InstanceType(
                typeInfo.vCpuInfo().defaultVCpus(),
                typeInfo.memoryInfo().sizeInMiB());
    }

    /**
     * Sets the desired size on the auto scaling group.
     *
     * @param newClusterSize new desired size to set
     */
    private static void setClusterDesiredSize(AutoScalingClient asClient, String asGroupName, int newClusterSize) {
        LOGGER.info("Setting auto scaling group {} desired size to {}", asGroupName, newClusterSize);
        asClient.setDesiredCapacity(req -> req.autoScalingGroupName(asGroupName).desiredCapacity(newClusterSize));
    }
}
