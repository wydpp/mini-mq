package com.dpp.minimq.broker.model;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description topic队列信息
 */
public class TopicQueueModel {
    private Integer id;
    private Long minOffset;
    private Long maxOffset;
    private Long currentOffset;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public Long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(Long currentOffset) {
        this.currentOffset = currentOffset;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TopicQueueModel{");
        sb.append("id=").append(id);
        sb.append(", minOffset=").append(minOffset);
        sb.append(", maxOffset=").append(maxOffset);
        sb.append(", currentOffset=").append(currentOffset);
        sb.append('}');
        return sb.toString();
    }
}
