package ru.ignatab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.service.TransactionProcessorService;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionConsumer {

  private final KafkaTemplate<String, Object> dlqKafkaTemplate;
  private final TransactionProcessorService processorService;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Value("${app.kafka.topics.dlq}")
  private String dlqTopic;

  @KafkaListener(topics = "${app.kafka.topics.main}", groupId = "${spring.kafka.consumer.group-id}")
  public void listen(String message) {
    try {
      TransactionDto dto = objectMapper.readValue(message, TransactionDto.class);
      processorService.processTransaction(dto);
    } catch (Exception ex) {
      log.error("Ошибка десериализации сообщения: {}", message, ex);
    }
  }

  private void sendToDlq(String message, String error) {
    try {
      var dlqMessage =
          Map.of(
              "originalMessage", message, "error", error, "timestamp", System.currentTimeMillis());

      dlqKafkaTemplate.send(dlqTopic, dlqMessage);
      System.out.println("Сообщение оправлено в DLQ " + dlqMessage);

    } catch (Exception ex) {
      System.err.println("Не удалось отправить в DLQ: " + ex.getMessage());
    }
  }
}
