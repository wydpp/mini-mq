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
package com.dpp.minimq.common.sysflag;


public class MessageSysFlag {
    /**
     * 该标志表示消息是非事务消息.
     * 对于普通的消息生产者发送的消息，通常会将其标记为非事务消息。
     */
    public final static int TRANSACTION_NOT_TYPE = 0;
    /**
     * 表示事务消息处于准备状态.
     * 这意味着该消息已经被发送到 RocketMQ 的 Broker，但尚未确定最终的事务结果，
     * 即该消息是否应该被提交（可以被消费者消费）还是被回滚（不允许被消费者消费）。
     */
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    /**
     * 此标志表示事务消息已经提交.
     * 表示事务执行成功，该消息可以被正常消费。
     */
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    /**
     * 表示事务消息需要进行回滚操作。
     */
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }


}
