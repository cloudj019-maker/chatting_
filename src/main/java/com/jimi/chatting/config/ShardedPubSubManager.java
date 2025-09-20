package com.jimi.chatting.config;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.event.connection.DisconnectedEvent;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ShardedPubSubManager implements AutoCloseable {

    private final RedisClusterClient client;
    private final StatefulRedisPubSubConnection<String, String> connection;
    private final ConcurrentHashMap<String, Sinks.Many<String>> channelSinks = new ConcurrentHashMap<>();

    private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet();

    public ShardedPubSubManager(String redisUri) {
        this.client = RedisClusterClient.create(redisUri);
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
    public void subscribeChannel(String channel) {
        channelSinks.computeIfAbsent(channel, ch -> Sinks.many().multicast().onBackpressureBuffer());
        connection.reactive().subscribe(channel).subscribe();
        subscribedChannels.add(channel);
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
        return channelSinks.get(channel).asFlux();
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }
}
