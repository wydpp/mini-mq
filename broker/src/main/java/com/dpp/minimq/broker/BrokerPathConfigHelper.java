/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dpp.minimq.broker;

import java.io.File;

public class BrokerPathConfigHelper {
    private static String brokerConfigPath = System.getProperty("user.home") + File.separator + "store"
        + File.separator + "config" + File.separator + "broker.properties";

    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public static void setBrokerConfigPath(String path) {
        brokerConfigPath = path;
    }

    public static String getTopicConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "topics.json";
    }

    public static String getTopicQueueMappingPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "topicQueueMapping.json";
    }

    public static String getConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerOffset.json";
    }

    public static String getLmqConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "lmqConsumerOffset.json";
    }

    public static String getConsumerOrderInfoPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerOrderInfo.json";
    }

    public static String getSubscriptionGroupPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "subscriptionGroup.json";
    }
    public static String getTimerCheckPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "timercheck";
    }
    public static String getTimerMetricsPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "timermetrics";
    }

    public static String getConsumerFilterPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerFilter.json";
    }

    public static String getMessageRequestModePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "messageRequestMode.json";
    }
}
