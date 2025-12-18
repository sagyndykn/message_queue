package com.nura.messagequeue.model.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "topics")
public class Topic {
    @Id
    private String id;

    @Indexed(unique = true)
    private String name;

    private Integer instances;

    private Integer maxSize;

    private String messageFormat;

    private Long createdAt;

    private Long updatedAt;

    public Topic() {
    }

    public Topic(String name, Integer instances, Integer maxSize) {
        this(name, instances, maxSize, "text");
    }

    public Topic(String name, Integer instances, Integer maxSize, String messageFormat) {
        this.name = name;
        this.instances = instances;
        this.maxSize = maxSize;
        this.messageFormat = messageFormat != null ? messageFormat : "text";
        long now = System.currentTimeMillis();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getInstances() {
        return instances;
    }

    public void setInstances(Integer instances) {
        this.instances = instances;
    }

    public Integer getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(Integer maxSize) {
        this.maxSize = maxSize;
    }

    public String getMessageFormat() {
        return messageFormat;
    }

    public void setMessageFormat(String messageFormat) {
        this.messageFormat = messageFormat;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", instances=" + instances +
                ", maxSize=" + maxSize +
                ", messageFormat='" + messageFormat + '\'' +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}