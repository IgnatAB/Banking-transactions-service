package ru.ignatab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.service.TransactionProcessorService;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionConsumer {

  private final TransactionProcessorService processorService;
  private final ObjectMapper objectMapper;

  @Value("${topics.dlq}")
  private String dlqTopic;

  @KafkaListener(topics = "${topics.main}", groupId = "${spring.kafka.consumer.group-id}")
  public void listen(String message) {
    try {
      TransactionDto dto = objectMapper.readValue(message, TransactionDto.class);
      processorService.processTransaction(dto);
    } catch (Exception ex) {
      log.error("Ошибка десериализации сообщения: {}", message, ex);
    }
  }
}
