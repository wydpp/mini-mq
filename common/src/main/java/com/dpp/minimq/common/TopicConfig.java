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
package com.dpp.minimq.common;

import com.alibaba.fastjson.TypeReference;

import java.util.HashMap;
import java.util.Map;

public class TopicConfig {
    private static final String SEPARATOR = " ";
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;
    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };
    //主题名称
    private String topicName;
    //主题的读队列数量
    private int readQueueNums = defaultReadQueueNums;
    //主题的写队列数量
    private int writeQueueNums = defaultWriteQueueNums;
    //是否是顺序消息
    private boolean order = false;
    // Field attributes should not have ' ' char in key or value, otherwise will lead to decode failure.
    private Map<String, String> attributes = new HashMap<>();

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    public static int getDefaultReadQueueNums() {
        return defaultReadQueueNums;
    }

    public static void setDefaultReadQueueNums(int defaultReadQueueNums) {
        TopicConfig.defaultReadQueueNums = defaultReadQueueNums;
    }

    public static int getDefaultWriteQueueNums() {
        return defaultWriteQueueNums;
    }

    public static void setDefaultWriteQueueNums(int defaultWriteQueueNums) {
        TopicConfig.defaultWriteQueueNums = defaultWriteQueueNums;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}
