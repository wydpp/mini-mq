package com.dpp.minimq.broker.core;

import java.util.HashMap;
import java.util.Map;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class MModelFileModelManager {
    /**
     * key:主题名称
     * value:主题对应的MMapFileModel对象
     */
    private Map<String, MMapFileModel> mMapFileModelMap = new HashMap<>();

    public void put(String topic, MMapFileModel mMapFileModel) {
        mMapFileModelMap.put(topic, mMapFileModel);
    }

    public MMapFileModel get(String topic) {
        return mMapFileModelMap.get(topic);
    }
}
