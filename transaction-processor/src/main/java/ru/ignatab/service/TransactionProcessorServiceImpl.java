package ru.ignatab.service;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.exception.BusinessValidationException;
import ru.ignatab.mapper.TransactionMapper;
import ru.ignatab.metrics.TransactionProcessorMetrics;
import ru.ignatab.model.Transaction;
import ru.ignatab.repository.TransactionRepository;

/**
 * Реализация сервиса обработки транзакций.
 *
 * <p>Сервис выполняет:
 *
 * <ul>
 *   <li>Бизнес-валидацию входящих транзакций
 *   <li>Сохранение успешных транзакций в базу данных
 *   <li>Отправку сообщений в Kafka в зависимости от статуса обработки: <b>approved</b>,
 *       <b>rejected</b> или <b>DLQ</b>
 * </ul>
 *
 * <p>Использует callback'и KafkaTemplate для отслеживания результата отправки сообщений.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionProcessorServiceImpl implements TransactionProcessorService {

  private final TransactionRepository repository;
  private final TransactionMapper transactionMapper;
  private final KafkaTemplate<String, TransactionDto> kafkaTemplate;
  private final TransactionProcessorMetrics metrics;

  @Value("${topics.approved}")
  private String approvedTopic;

  @Value("${topics.rejected}")
  private String rejectedTopic;

  @Value("${topics.dlq}")
  private String dlqTopic;

  @Override
  @Transactional
  public void processTransaction(TransactionDto transaction) {
    try {
      log.info(
          "Processing transaction id={}, type={}, status={}",
          transaction.transactionId(),
          transaction.type(),
          transaction.status());

      if (transaction.status() == TransactionStatus.FAILED) {
        log.info(
            "Incoming transaction has status FAILED — routing to rejected-transaction topic: {}",
            transaction.transactionId());
        sendToRejected(
            transaction,
            transaction.errorMessage() != null
                ? transaction.errorMessage()
                : "Initial status FAILED");
        log.info("Processing finished for transaction {}", transaction.transactionId());
        return;
      }

      if (transaction.status() == TransactionStatus.SUCCESS) {
        validateTransaction(transaction);

        Transaction entity = transactionMapper.toEntity(transaction);
        repository.save(entity);
        metrics.incrementDb();
        log.info("Transaction {} saved to DB", transaction.transactionId());

        TransactionDto approved = buildTransaction(transaction, TransactionStatus.SUCCESS, null);

        sendToTopicWithCallback(
            approvedTopic,
            transaction.transactionId().toString(),
            approved,
            metrics::incrementApproved,
            ex -> {
              log.error(
                  "Failed to send approved for tx {}, fallback to DLQ. Reason: {}",
                  transaction.transactionId(),
                  ex.getMessage(),
                  ex);
              sendToDlq(transaction, "Failed to send approved: " + ex.getMessage());
            });
        log.info("Transaction {} routing to approve-transaction topic ", transaction.transactionId());
        log.info("Processing finished for transaction {}", transaction.transactionId());
        return;
      }

      log.warn(
          "Unknown transaction status {} for id={}, routing to DLQ",
          transaction.status(),
          transaction.transactionId());
      sendToDlq(transaction, "Unknown status: " + transaction.status());

    } catch (BusinessValidationException e) {
      log.warn(
          "Business rule violated for transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage());
      sendToRejected(transaction, e.getMessage());

    } catch (DataAccessException e) {
      log.error(
          "Database error while processing transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
      sendToDlq(transaction, "Database error: " + e.getMessage());

    } catch (KafkaException e) {
      log.error(
          "Kafka error while processing transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
      sendToDlq(transaction, "Kafka error: " + e.getMessage());

    } catch (Exception e) {
      log.error(
          "Unexpected error while processing transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
      sendToDlq(transaction, "Unexpected error: " + e.getMessage());
    }
  }

  /**
   * Отправляет транзакцию в топик rejected.
   *
   * @param transaction исходная транзакция
   * @param errorMessage описание причины отклонения
   */
  public void sendToRejected(TransactionDto transaction, String errorMessage) {
    TransactionDto rejected = buildTransaction(transaction, TransactionStatus.FAILED, errorMessage);

    Message<TransactionDto> message =
        MessageBuilder.withPayload(rejected)
            .setHeader("kafka_messageKey", transaction.transactionId().toString())
            .setHeader("x-origin-service", "transaction-processor")
            .setHeader("x-error", errorMessage)
            .setHeader("kafka_topic", rejectedTopic)
            .build();

    sendToTopicWithCallbackMessage(
        rejectedTopic,
        transaction.transactionId().toString(),
        message,() -> {
          metrics.incrementRejected();
          log.warn(
              "Transaction id={} sent to rejected-transaction topic, reason={}",
              transaction.transactionId(),
              errorMessage);
        },
        ex -> {
          log.error(
              "Failed to send transaction id={} to rejected topic, reason={}",
              transaction.transactionId(),
              ex.getMessage(),
              ex);
          sendToDlq(transaction, "Rejected fallback error: " + ex.getMessage());
        });
  }

  /**
   * Отправляет транзакцию в DLQ (Dead Letter Queue).
   *
   * @param transaction исходная транзакция
   * @param errorMessage причина помещения в DLQ
   */
  public void sendToDlq(TransactionDto transaction, String errorMessage) {
    TransactionDto failed = buildTransaction(transaction, TransactionStatus.FAILED, errorMessage);

    Message<TransactionDto> message =
        MessageBuilder.withPayload(failed)
            .setHeader("kafka_messageKey", transaction.transactionId().toString())
            .setHeader("x-origin-service", "transaction-processor")
            .setHeader("x-error", errorMessage)
            .setHeader("kafka_topic", dlqTopic)
            .build();

    sendToTopicWithCallbackMessage(
        dlqTopic,
        transaction.transactionId().toString(),
        message,
        () -> {
          metrics.incrementDlq();
          log.warn(
              "Transaction id={} sent to DLQ, reason={}",
              transaction.transactionId(),
              errorMessage);
        },
        ex ->
            log.error(
                "Failed to send tx {} to DLQ, reason={}",
                transaction.transactionId(),
                ex.getMessage(),
                ex));
  }

  /** Отправка сообщения в Kafka без дополнительных заголовков. */
  private void sendToTopicWithCallback(
      String topic,
      String key,
      TransactionDto payload,
      Runnable onSuccess,
      java.util.function.Consumer<Throwable> onFailure) {

    CompletableFuture<SendResult<String, TransactionDto>> future =
        kafkaTemplate.send(topic, key, payload);

    future.whenComplete(
        (result, ex) -> {
          if (ex != null) {
            onFailure.accept(ex);
          } else {
            onSuccess.run();
          }
        });
  }

  /** Отправка сообщения в Kafka с заголовками. */
  private void sendToTopicWithCallbackMessage(
      String topic,
      String key,
      Message<TransactionDto> message,
      Runnable onSuccess,
      java.util.function.Consumer<Throwable> onFailure) {

    CompletableFuture<SendResult<String, TransactionDto>> future = kafkaTemplate.send(message);

    future.whenComplete(
        (result, ex) -> {
          if (ex != null) {
            onFailure.accept(ex);
          } else {
            onSuccess.run();
          }
        });
  }

  /** Создает новый экземпляр TransactionDto с обновленным статусом и сообщением об ошибке. */
  private TransactionDto buildTransaction(
      TransactionDto original, TransactionStatus status, String errorMessage) {

    return new TransactionDto(
        original.transactionId(),
        original.clientId(),
        original.fromAccount(),
        original.toAccount(),
        original.type(),
        original.amount(),
        original.createdAt(),
        status,
        errorMessage);
  }

  /** Выполняет бизнес-валидацию полей транзакции. */
  private void validateTransaction(TransactionDto transaction) {
    if (transaction.amount() == null || transaction.amount().compareTo(BigDecimal.ZERO) <= 0) {
      throw new BusinessValidationException("Сумма транзакции должна быть больше 0");
    }
    if (Objects.equals(transaction.fromAccount(), transaction.toAccount())) {
      throw new BusinessValidationException("Счета отправителя и получателя совпадают");
    }
  }
}
