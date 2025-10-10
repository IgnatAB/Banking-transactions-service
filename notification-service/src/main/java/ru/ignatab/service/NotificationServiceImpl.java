package ru.ignatab.service;

import java.util.Random;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.exception.NotificationSendException;
import ru.ignatab.metrics.NotificationMetrics;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationServiceImpl implements NotificationService {

  public final NotificationMetrics metrics;
  private final KafkaTemplate<String, TransactionDto> kafkaTemplate;

  @Value("${topics.dlq}")
  private String dlqTopic;

  Random random = new Random();

  @Override
  public void sendApprovedNotification(TransactionDto transaction) {
    try {
      simulateRandomFailure(transaction);
      log.info(
          "[Notify] Клиент {}: транзакция {} на сумму {}, успешно обработана!",
          transaction.clientId(),
          transaction.transactionId(),
          transaction.amount());

      metrics.incrementApproved();
    } catch (NotificationSendException e) {
      sendToDlq(transaction, e.getMessage());
    }
  }

  @Override
  public void sendRejectedNotification(TransactionDto transaction) {
    try {
      log.info(
          "[Notify] Клиент {}: транзакция {} на сумму {} отклонена по причине {}",
          transaction.clientId(),
          transaction.transactionId(),
          transaction.amount(),
          transaction.errorMessage());

      metrics.incrementRejected();
    } catch (NotificationSendException e) {
      sendToDlq(transaction, e.getMessage());
    }
  }

  private void simulateRandomFailure(TransactionDto transaction) {

    if (random.nextInt(10) == 0) { // ~10% вероятности ошибки
      throw new NotificationSendException(
          "Ошибка отправки уведомления клиенту " + transaction.clientId());
    }
  }

  private void sendToDlq(TransactionDto transaction, String reason) {
    try {
      kafkaTemplate.send(dlqTopic, transaction.transactionId().toString(), transaction);
      log.warn(
          "Уведомление отправлено в DLQ: {}, причина: {}", transaction.transactionId(), reason);
      metrics.incrementDlq();
    } catch (Exception e) {
      log.error("Ошибка отправки в DLQ: {}", e.getMessage(), e);
    }
  }
}
