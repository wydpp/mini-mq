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
package com.dpp.minimq.store;

/**
 * Clean up page-table on super large disk
 * 定义一种对象可以在不同的存储介质（如内存和磁盘）之间进行交换的能力。
 * 这种交换一般是指在内存和持久化存储之间切换数据的存储方式
 */
public interface Swappable {
    /**
     * 将内存中的某些数据交换到磁盘中
     * 根据 reserveNum 判断是否需要强制交换
     * 如果需要强制交换，则按照 forceSwapIntervalMs 进行交换。
     * 否则，按照 normalSwapIntervalMs 进行正常交换。
     * @param reserveNum
     * @param forceSwapIntervalMs
     * @param normalSwapIntervalMs
     */
    void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs);

    /**
     * 清理已经被交换到磁盘的数据
     * @param forceCleanSwapIntervalMs
     */
    void cleanSwappedMap(long forceCleanSwapIntervalMs);
}
