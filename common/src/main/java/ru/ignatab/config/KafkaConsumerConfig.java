package ru.ignatab.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.ignatab.dto.TransactionDto;

/**
 * Конфигурация Kafka Consumer для переиспользования в сервисах transaction-processor и
 * notification-service
 *
 * <p>Настраивает фабрику консьюмеров и сериализацию сообщений в формате JSON. Поддерживает
 * обработку ошибок десериализации через ErrorHandlingDeserializer.
 */
@Configuration
public class KafkaConsumerConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Bean
  public ConsumerFactory<String, TransactionDto> consumerFactory(ObjectMapper objectMapper) {
    JsonDeserializer<TransactionDto> jsonDeserializer =
        new JsonDeserializer<>(TransactionDto.class, objectMapper, false);
    jsonDeserializer.addTrustedPackages("*");

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, jsonDeserializer);

    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new ErrorHandlingDeserializer<>(jsonDeserializer));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, TransactionDto>
      kafkaListenerContainerFactory(ObjectMapper objectMapper) {
    ConcurrentKafkaListenerContainerFactory<String, TransactionDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory(objectMapper));
    return factory;
  }
}
