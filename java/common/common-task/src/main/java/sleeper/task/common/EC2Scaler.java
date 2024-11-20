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

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.SetDesiredCapacityRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesResult;
import com.amazonaws.services.ec2.model.InstanceTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;

/**
 * ECS EC2 auto scaler. This makes decisions on how many instances to start and stop based on the
 * amount of work there is to do.
 */
public class EC2Scaler {

    private static final Logger LOGGER = LoggerFactory.getLogger(EC2Scaler.class);

    private EC2Scaler() {
    }

    public static CompactionTaskHostScaler create(InstanceProperties instanceProperties, AmazonAutoScaling asClient, AmazonEC2 ec2Client) {
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
    private static int getAutoScalingGroupMaxSize(String groupName, AmazonAutoScaling client) {
        DescribeAutoScalingGroupsRequest req = new DescribeAutoScalingGroupsRequest()
                .withAutoScalingGroupNames(groupName)
                .withMaxRecords(1);
        DescribeAutoScalingGroupsResult result = client.describeAutoScalingGroups(req);
        if (result.getAutoScalingGroups().size() != 1) {
            throw new IllegalStateException("instead of 1, received " + result.getAutoScalingGroups().size()
                    + " records for describe_auto_scaling_groups on group name " + groupName);
        }
        AutoScalingGroup group = result.getAutoScalingGroups().get(0);
        LOGGER.debug("Auto scaling group instance count: minimum {}, desired size {}, maximum size {}",
                group.getMinSize(), group.getDesiredCapacity(), group.getMaxSize());
        return group.getMaxSize();
    }

    private static CompactionTaskHostScaler.InstanceType getEc2InstanceType(AmazonEC2 ec2Client, String instanceType) {
        DescribeInstanceTypesRequest request = new DescribeInstanceTypesRequest().withInstanceTypes(instanceType);
        DescribeInstanceTypesResult result = ec2Client.describeInstanceTypes(request);
        if (result.getInstanceTypes().size() != 1) {
            throw new IllegalStateException("got more than 1 result for DescribeInstanceTypes for type " + instanceType);
        }
        InstanceTypeInfo typeInfo = result.getInstanceTypes().get(0);
        LOGGER.info("EC2 instance type info: {}", typeInfo);
        return new CompactionTaskHostScaler.InstanceType(
                typeInfo.getVCpuInfo().getDefaultVCpus(),
                typeInfo.getMemoryInfo().getSizeInMiB());
    }

    /**
     * Sets the desired size on the auto scaling group.
     *
     * @param newClusterSize new desired size to set
     */
    private static void setClusterDesiredSize(AmazonAutoScaling asClient, String asGroupName, int newClusterSize) {
        LOGGER.info("Setting auto scaling group {} desired size to {}", asGroupName, newClusterSize);
        SetDesiredCapacityRequest req = new SetDesiredCapacityRequest()
                .withAutoScalingGroupName(asGroupName)
                .withDesiredCapacity(newClusterSize);
        asClient.setDesiredCapacity(req);
    }
}
