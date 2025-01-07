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

package com.dpp.minimq.common.protocol;

/**
 * 消息请求码
 */
public class RequestCode {
    /**
     * 发送消息
     */
    public static final int SEND_MESSAGE = 10;
    /**
     * 拉去消息
     */
    public static final int PULL_MESSAGE = 11;
    /**
     * 查询消息
     */
    public static final int QUERY_MESSAGE = 12;
    /**
     * 查询broker offset
     */
    public static final int QUERY_BROKER_OFFSET = 13;
    /**
     * 查询consumer offset
     */
    public static final int QUERY_CONSUMER_OFFSET = 14;
}
