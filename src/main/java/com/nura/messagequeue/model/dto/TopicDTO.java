package com.nura.messagequeue.model.dto;

public class TopicDTO {
    private String name;
    private int instances;
    private int maxSize;
    private String messageFormat;

    public TopicDTO() {
    }

    public TopicDTO(String name, int instances, int maxSize, String messageFormat) {
        this.name = name;
        this.instances = instances;
        this.maxSize = maxSize;
        this.messageFormat = messageFormat;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getInstances() {
        return instances;
    }

    public void setInstances(int instances) {
        this.instances = instances;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public String getMessageFormat() {
        return messageFormat;
    }

    public void setMessageFormat(String messageFormat) {
        this.messageFormat = messageFormat;
    }
}
