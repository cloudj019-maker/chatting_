package com.jimi.chatting.config;

import io.lettuce.core.pubsub.RedisPubSubListener;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public record RedisPubSubListenerImpl(
        ConcurrentHashMap<String, Sinks.Many<String>> channelSinks) implements RedisPubSubListener<String, String> {

    @Override
    public void message(String channel, String message) {
        Sinks.Many<String> sink = channelSinks.get(channel);
        if (channel != null) {
            sink.tryEmitNext(message);
        }

        log.error("sink doesn't exist");
    }

    @Override
    public void message(String pattern, String channel, String message) {

    }

    @Override
    public void subscribed(String channel, long count) {
    }

    @Override
    public void psubscribed(String pattern, long count) {

    }

    @Override
    public void unsubscribed(String channel, long count) {

    }

    @Override
    public void punsubscribed(String pattern, long count) {

    }
}
