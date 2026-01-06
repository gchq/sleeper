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

/**
 * Implements a plugin for Trino which reads its data from a Sleeper instance running in AWS. The entry point for the
 * plugin is {@link sleeper.trino.SleeperPlugin}. This is a very good place to start if you wish to understand the
 * operation of this plugin.
 * <p>
 * The entry point for this plugin is specified within the resources folder, inside the file
 * META-INF.services.io.trino.spi.Plugin.
 * <p>
 * This plugin is configured using the standard Trino configuration system. Typically this involves creating a file
 * called ${TRINO_HOME}/etc/config/sleeper.properties. This properties file contains configuration lines such as:
 *
 * <pre>
 *     connector.name=sleeper
 *     sleeper.configbucket=my-sleeper-configbucket-in-aws
 *     sleeper.rowsperpage=4096
 * </pre>
 *
 * The values from this file are loaded using the {@link sleeper.trino.SleeperConfig} class. Refer to that class for
 * details about what each parameter means.
 * <p>
 * Trino will look for the JAR files for this plugin in ${TRINO_HOME}/plugin/sleeper. All of the JAR files in that
 * directory are placed on the classpath for the Sleeper plugin. Each Trino plugin has its own classloader and so there
 * is reduced opportunity for conflict.
 * <p>
 * This plugin will retrieve all of the configuration information for the Sleeper instance from the config bucket which
 * has been specified in the sleeper.properties file. It uses the default AWS credentials to access this bucket (and all
 * other buckets of this Sleeper instance) and so make sure that these are set up before you run this plugin. This is
 * usually achieved using a sequence of 'aws configure' commands.
 * <p>
 * This plugin supports:
 * <ul>
 * <li>Predicate push-down of rowkey columns</li>
 * <li>Dynamic-filter joins</li>
 * </ul>
 * <p>
 * Known major shortcomings of this plugin:
 * <ul>
 * <li>Only single-valued row keys are supported. Multi-valued row keys are very hard to implement efficiently in Trino
 * because they cause the {@link io.trino.spi.predicate.TupleDomain} to become incredibly imprecise and scan far too
 * much data</li>
 * <li>No automated testing</li>
 * <li>No transaction consistency between tables, which is a current shortcoming of Sleeper</li>
 * </ul>
 * <p>
 * The code style of this plugin follows the code style of many other Trino plugins, wherever this is possible.
 * The key features of this style are:
 * <ul>
 * <li>Dependency injection using Guice. It is strange at first sight but works very well</li>
 * <li>Immutable data wherever possible, often using Guava immutable collections</li>
 * <li>Extensive use of Java Streams, such as map, filter and reduce</li>
 * </ul>
 */
package sleeper.trino;
