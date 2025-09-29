package com.jimi.chatting.kafkaConfig;

import lombok.Value;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class MessagingProduceConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public SenderOptions<String, byte[]> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 리더, 최소 ISR 모두 기록 확인 후 응답
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // 중복 없는 전송 보장
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // 전송 실패 시 무한에 가깝게 재시더
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 해당 시간 경과 시, 배치가 차지않아도 전송
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768); // 배치 상한 크기
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); //
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000); // 10s를 기다렸다가 drop

        return SenderOptions.create(props);
    }

    @Bean
    public KafkaSender<String, byte[]> kafkaSender(SenderOptions<String, byte[]> senderOptions) {
        return KafkaSender.create(senderOptions);
    }
}
