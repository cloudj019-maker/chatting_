package com.jimi.chatting.component;

import com.jimi.chatting.dto.ChatMessage;
import com.jimi.chatting.redisConfig.ShardedPubSubManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Component
public class ChatOutbound {

    private final ShardedPubSubManager shardedPubSubManager;

    public ChatOutbound(ShardedPubSubManager shardedPubSubManager) {
        this.shardedPubSubManager = shardedPubSubManager;
    }

    public Mono<Void> handleOutbound(WebSocketSession session, String roomId) {
        return sessionConnected(session, roomId)
                .then(session.send(
                        shardedPubSubManager.messageStream(roomId)
                                .map(session::textMessage)
                ));
    }

    public Mono<Void> sessionConnected(WebSocketSession session, String roomId) {
        return shardedPubSubManager.subscribeChannel(roomId)
                .doOnSuccess(v -> log.info("Subscribe channel success"))
                .onErrorResume(err -> {
                    log.error("Subscribe error for roomId={}", roomId, err);
                    return session.close(CloseStatus.PROTOCOL_ERROR);
                });
    }
}
