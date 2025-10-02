package ru.ignatab.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

  /** Адрес Kafka брокера */
  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Bean
  public ProducerFactory<String, Object> producerFactory() {
    Map<String, Object> configProps = new HashMap<>();

    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    /** Ключ будет сереализован как строка */
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    /** Значение сообщения будет сереализовано в json */
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    /**
     * Настройки для JSON сериализатора: JsonSerializer добавляет metadata, чтобы Kafka знала тип
     * объекта
     */
    configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

    /**
     * Настройки надежности доставки: ACKS_CONFIG=all- продюсер ждет подтверждения от всех реплик
     */
    configProps.put(ProducerConfig.ACKS_CONFIG, "all");

    /** RETRIES_CONFIG количество ретраев при отправке */
    configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

    /** LINGER_MS_CONFIG - задержка перед отправкой для батчинга */
    configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);

    return new DefaultKafkaProducerFactory<>(configProps);
  }

  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate() {

    return new KafkaTemplate<>(producerFactory());
  }
}
