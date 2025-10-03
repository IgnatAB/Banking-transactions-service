package ru.ignatab.config;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.ignatab.dto.TransactionDto;

@Slf4j
@EnableKafka
@Configuration
public class KafkaProducerConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  /** Producer factory для dql */
  @Bean
  public ProducerFactory<String, Object> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

    return new DefaultKafkaProducerFactory<>(config);
  }

  /**
   * KafkaTemplate для {@link ru.ignatab.service.TransactionProcessorServiceImpl} (сервис будет
   * писать туда при ошибках в транзакциях)
   */
  @Bean
  public KafkaTemplate<String, TransactionDto> kafkaTemplateTransactionDto(
      ProducerFactory<String, TransactionDto> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  /** KafkaTemplate для dql (сервис будет писать туда при ошибках в транзакциях) */
  @Bean
  public KafkaTemplate<String, Object> dlqkafkaTemplate() {

    return new KafkaTemplate<>(producerFactory());
  }
}
