package com.jimi.chatting.redisConfig;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.event.connection.DisconnectedEvent;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Configuration
public class ShardedPubSubManager implements AutoCloseable {

    private final RedisClusterClient client;
    private final StatefulRedisPubSubConnection<String, String> connection;
    private final ConcurrentHashMap<String, Sinks.Many<String>> channelSinks = new ConcurrentHashMap<>();

    private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet();

    public ShardedPubSubManager(@Value("${chat.redis.cluster.nodes}") List<String> nodes) {
        this.client = RedisClusterClient.create((RedisURI) nodes);
        this.connection = client.connectPubSub();

        connection.addListener(new RedisPubSubListenerImpl(channelSinks));

        ClientResources resources = client.getResources();
        EventBus eventBus = resources.eventBus();
        eventBus.get()
                .ofType(DisconnectedEvent.class)
                .subscribe(event -> {
                    System.out.println("Redis disconnected, disposing all subscriptions");
                    resubscribeAll();
                });
    }

    // 채널 구독
    public Mono<Void> subscribeChannel(String channel) {
        channelSinks.computeIfAbsent(channel, ch -> Sinks.many().multicast().onBackpressureBuffer());
        return connection.reactive()
                .psubscribe(channel)
                .doOnSuccess(v -> subscribedChannels.add(channel))
                .doOnError(err -> log.error("Subscribe error", err));
    }

    // 메시지 발행
    public void publish(String channel, String message) {
        connection.reactive().publish(channel, message).subscribe();
    }

    // 자동 재구독
    private void resubscribeAll() {
        subscribedChannels.forEach(channel -> connection.reactive().subscribe(channel).subscribe());
    }

    public Flux<String> messageStream(String channel) {
        return channelSinks.computeIfAbsent(channel, ch -> Sinks.many().multicast().onBackpressureBuffer()).asFlux();
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }
}
