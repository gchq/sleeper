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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public abstract class AbstractEmrExecutor extends Executor {

    public AbstractEmrExecutor(
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            AmazonS3 amazonS3Client) {
        super(instanceProperties, tablePropertiesProvider, amazonS3Client);
    }

    @Override
    protected List<String> constructArgs(BulkImportJob bulkImportJob) {
        List<String> args = super.constructArgs(bulkImportJob);
        args.add(bulkImportJob.getId());
        args.add(instanceProperties.get(CONFIG_BUCKET));
        return args;
    }

    @Override
    protected String getJarLocation() {
        return "s3a://"
                + instanceProperties.get(UserDefinedInstanceProperty.JARS_BUCKET)
                + "/bulk-import-runner-"
                + instanceProperties.get(SystemDefinedInstanceProperty.VERSION) + ".jar";
    }
}
