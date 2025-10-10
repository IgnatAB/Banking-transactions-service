package ru.ignatab.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.service.TransactionProcessorService;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionListener {

  private final TransactionProcessorService processorService;

  @KafkaListener(topics = "${topics.main}", groupId = "${spring.kafka.consumer.group-id}")
  public void listen(TransactionDto transactionDto) {
    try {
      processorService.processTransaction(transactionDto);
    } catch (Exception ex) {
      log.error("Ошибка десериализации сообщения: {}", transactionDto, ex);
    }
  }
}
