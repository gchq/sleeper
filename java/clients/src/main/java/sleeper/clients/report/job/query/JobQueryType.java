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
package sleeper.clients.report.job.query;

/**
 * Types of query for a job to include in reports. This is used for both compaction and ingest.
 */
public class JobQueryType {

    public static final JobQueryType PROMPT = builder()
            .shortName("p")
            .name("prompt")
            .factory(JobQueryFactory.fromPrompts())
            .build();
    public static final JobQueryType ALL = builder()
            .shortName("a")
            .name("all")
            .factory(JobQueryFactory.fromTableStatus(AllJobsQuery::new))
            .build();
    public static final JobQueryType DETAILED = builder()
            .shortName("d")
            .name("detailed")
            .factory(JobQueryFactory.fromParameters(DetailedJobsQuery::fromParameters, DetailedJobsQuery::prompt))
            .build();
    public static final JobQueryType RANGE = builder()
            .shortName("r")
            .name("range")
            .factory((reader, parameters) -> {
                if (parameters == null) {
                    return RangeJobsQuery.prompt(reader.getTableStatus(), reader.getConsole(), reader.getClock());
                } else {
                    return RangeJobsQuery.fromParameters(reader.getTableStatus(), parameters, reader.getClock());
                }
            })
            .build();
    public static final JobQueryType REJECTED = builder()
            .shortName("n")
            .name("rejected")
            .factory(JobQueryFactory.from(RejectedJobsQuery::new))
            .build();

    private final String shortName;
    private final String name;
    private final JobQueryFactory factory;

    private JobQueryType(Builder builder) {
        shortName = builder.shortName;
        name = builder.name;
        factory = builder.factory;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getShortName() {
        return shortName;
    }

    public String getName() {
        return name;
    }

    public JobQueryFactory getFactory() {
        return factory;
    }

    /**
     * A builder for job query types.
     */
    public static class Builder {
        private String shortName;
        private String name;
        private JobQueryFactory factory;

        /**
         * Sets the short name to identify the query type in a command line argument or prompt.
         *
         * @param  shortName the short name, usually a single letter
         * @return           this builder
         */
        public Builder shortName(String shortName) {
            this.shortName = shortName;
            return this;
        }

        /**
         * Sets the name of the query type when describing it to the user.
         *
         * @param  name the name
         * @return      this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the factory to create a job query of this type.
         *
         * @param  factory the factory
         * @return         this builder
         */
        public Builder factory(JobQueryFactory factory) {
            this.factory = factory;
            return this;
        }

        public JobQueryType build() {
            return new JobQueryType(this);
        }

    }

}
