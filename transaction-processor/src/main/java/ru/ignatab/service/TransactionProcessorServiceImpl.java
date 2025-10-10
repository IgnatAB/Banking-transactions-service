package ru.ignatab.service;

import java.math.BigDecimal;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.exception.BusinessValidationException;
import ru.ignatab.mapper.TransactionMapper;
import ru.ignatab.metrics.TransactionProcessorMetrics;
import ru.ignatab.model.Transaction;
import ru.ignatab.repository.TransactionRepository;

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

      if (transaction.amount().compareTo(BigDecimal.ZERO) <= 0) {
        throw new BusinessValidationException("Сумма транзакции должна быть больше 0");
      }
      if (Objects.equals(transaction.fromAccount(), transaction.toAccount())) {
        throw new BusinessValidationException("Счета отправителя и получателя совпадают");
      }
      if (transaction.status() == TransactionStatus.SUCCESS) {
        Transaction entity = transactionMapper.toEntity(transaction);
        repository.save(entity);
        metrics.incrementDb();

        log.info("Transaction {} processed successfully", transaction.transactionId());

        TransactionDto approved = buildTransaction(transaction, TransactionStatus.SUCCESS, null);
        kafkaTemplate.send(approvedTopic, transaction.transactionId().toString(), approved);
        metrics.incrementApproved();
        log.info(
            "Transaction id={} sent to topic '{}'", transaction.transactionId(), approvedTopic);
      }

    } catch (BusinessValidationException e) {
      log.warn(
          "Business rule violated for transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage());
      sendToRejected(transaction, e.getMessage());
      metrics.incrementRejected();

    } catch (DataAccessException e) {
      log.error(
          "Database error while processing transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
      sendToDlq(transaction, "Database error: " + e.getMessage());
      metrics.incrementDlq();

    } catch (KafkaException e) {
      log.error(
          "Kafka error while processing transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
      sendToDlq(transaction, "Kafka error: " + e.getMessage());
      metrics.incrementDlq();

    } catch (Exception e) {

      log.error(
          "Unexpected error while processing transaction id={}, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
      sendToDlq(transaction, "Unexpected error: " + e.getMessage());
      metrics.incrementDlq();
    }
  }

  public void sendToRejected(TransactionDto transaction, String errorMessage) {
    try {
      TransactionDto rejected =
          buildTransaction(transaction, TransactionStatus.FAILED, errorMessage);
      kafkaTemplate.send(rejectedTopic, transaction.transactionId().toString(), rejected);
      metrics.incrementRejected();
      log.info("Transaction id={} sent to topic '{}'", transaction.transactionId(), rejectedTopic);
    } catch (Exception e) {
      log.error(
          "Failed to send transaction id={} to rejected topic, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);

      sendToDlq(transaction, "Rejected fallback error: " + e.getMessage());
      metrics.incrementDlq();
    }
  }

  public void sendToDlq(TransactionDto transaction, String errorMessage) {
    try {
      TransactionDto failed = buildTransaction(transaction, TransactionStatus.FAILED, errorMessage);
      kafkaTemplate.send(dlqTopic, transaction.transactionId().toString(), failed);
      metrics.incrementDlq();
      log.warn(
          "Transaction id={} sent to DLQ, reason={}", transaction.transactionId(), errorMessage);
    } catch (Exception e) {
      log.error(
          "Failed to send transaction id={} to DLQ, reason={}",
          transaction.transactionId(),
          e.getMessage(),
          e);
    }
  }

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
}
