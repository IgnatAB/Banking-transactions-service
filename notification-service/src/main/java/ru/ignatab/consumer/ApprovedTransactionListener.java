package ru.ignatab.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.service.NotificationServiceImpl;

@Slf4j
@Component
@RequiredArgsConstructor
public class ApprovedTransactionListener {

  private final NotificationServiceImpl notificationService;

  @KafkaListener(topics = "${topics.approved}", groupId = "${spring.kafka.consumer.group-id}")
  public void listenApproved(TransactionDto transaction) {

    log.info("Получено сообщение из approved-transactions: {}", transaction);
    notificationService.sendApprovedNotification(transaction);
  }
}
