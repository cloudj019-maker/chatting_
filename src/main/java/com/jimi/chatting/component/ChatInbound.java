package com.jimi.chatting.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jimi.chatting.dto.ChatMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
public class ChatInbound {

    private final KafkaSender<String, byte[]> kafkaSender;
    private final ObjectMapper objectMapper;
    private final String topic;

    public ChatInbound(KafkaSender<String, byte[]> kafkaSender, ObjectMapper objectMapper,
                       @Value("{${chat.kafka.topic}") String topic)  {
        this.kafkaSender = kafkaSender;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public Mono<Void> handleInbound(WebSocketSession session) {
        Flux<ChatMessage> inbound = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .map(this::decode)
                .onBackpressureBuffer(1000,
                        dropped -> log.warn("WS inbound overflow, dropped message"),
                        BufferOverflowStrategy.DROP_OLDEST);

    return inbound
            .groupBy(ChatMessage::roomId)
            .flatMap(group ->
                    group.concatMap(this::sendOne), 64)
            .then();
    }

    private Mono<Void> sendOne(ChatMessage msg) {
        return Mono.defer(() -> {
            byte[] value;
            try {
                value = objectMapper.writeValueAsBytes(msg);
            } catch (Exception e) {
                return Mono.error(new IllegalArgumentException("Serialize failed", e));
            }
            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(topic, msg.roomId(), value);

            SenderRecord<String, byte[], String> sr =
                    SenderRecord.create(record, msg.roomId());

            return kafkaSender.send(Mono.just(sr))
                    .next() // 단건 결과만
                    .flatMap(res -> {
                        if (res.exception() != null) {
                            return Mono.error(res.exception());
                        }
                        return Mono.empty();
                    });
        });
    }

    private ChatMessage decode(String json) {
        try {
            return objectMapper.readValue(json, ChatMessage.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid inbound JSON", e);
        }
    }
}
