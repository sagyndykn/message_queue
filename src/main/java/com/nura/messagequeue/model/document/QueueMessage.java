package com.nura.messagequeue.model.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "queue_messages")
@CompoundIndex(name = "topic_created_id_idx", def = "{'topic': 1, 'createdAt': 1, '_id': 1}")
public class QueueMessage {
    @Id
    private String id;

    @Indexed
    private String topic;

    private String message;

    private Long createdAt;

    public QueueMessage() {
    }

    public QueueMessage(String topic, String message) {
        this.topic = topic;
        this.message = message;
        this.createdAt = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "QueueMessage{" +
                "id=" + id +
                ", topic='" + topic + '\'' +
                ", message='" + message + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}
