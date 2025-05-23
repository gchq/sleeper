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
package sleeper.clients.api.role;

import org.apache.hadoop.conf.Configuration;

public class AssumeSleeperRoleHadoop {
    private final String roleArn;
    private final String roleSessionName;

    AssumeSleeperRoleHadoop(String roleArn, String roleSessionName) {
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
    }

    public Configuration setS3ACredentials(Configuration configuration) {
        String originalCredentialsProvider = configuration.get("fs.s3a.aws.credentials.provider");
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
        configuration.set("fs.s3a.assumed.role.arn", roleArn);
        configuration.set("fs.s3a.assumed.role.session.name", roleSessionName);
        configuration.set("fs.s3a.assumed.role.credentials.provider", originalCredentialsProvider);
        return configuration;
    }
}
