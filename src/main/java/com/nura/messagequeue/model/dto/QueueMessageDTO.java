package com.nura.messagequeue.model.dto;

public class QueueMessageDTO {
    private String topic;
    private String message;

    public QueueMessageDTO() {
    }

    public QueueMessageDTO(String topic, String message) {
        this.topic = topic;
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
