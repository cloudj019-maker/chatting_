package com.jimi.chatting.handler;

import com.jimi.chatting.component.ChatInbound;
import com.jimi.chatting.component.ChatOutbound;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

import java.net.URI;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChatWebSocketHandler implements WebSocketHandler {

    private final ChatInbound chatInbound;
    private final ChatOutbound chatOutbound;


    @Override
    public Mono<Void> handle(WebSocketSession session) {
        URI uri = session.getHandshakeInfo().getUri();
        HttpHeaders headers = session.getHandshakeInfo().getHeaders();

        String path = uri.getPath();
        String roomId = path.substring(path.lastIndexOf('/') + 1);

        Mono<Void> inbound = chatInbound.handleInbound(session);
        Mono<Void> outbound = chatOutbound.handleOutbound(session, roomId);

        return Mono.when(inbound, outbound);
    }
}
