package com.jimi.chatting.config;

import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

public class WebSocketRoutingConfig {

    @Bean
    SimpleUrlHandlerMapping simpleUrlHandlerMapping() {}
}
