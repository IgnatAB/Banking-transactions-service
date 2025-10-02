package ru.ignatab.service;

import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.ignatab.config.GeneratorProperties;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.enums.TransactionType;

/** Реализация сервиса по генерации транзакций */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionGeneratorServiceImpl implements TransactionGeneratorService {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final GeneratorProperties properties;
  private final Random random = new Random();
  private List<UUID> clientPool;

  @PostConstruct
  public void initClientPool(){

    List<UUID> pool = new ArrayList<>();
    for(int i =0 ; i< properties.getClients(); i++) {
      pool.add(UUID.randomUUID());
    }
    this.clientPool = Collections.unmodifiableList(pool);
  }

  @Override
  @PostConstruct
  public void startGenerating() {

    log.info("Создание пула клиентов");
    initClientPool();

    log.info("Запуск генерации транзакций");
    ExecutorService executor = Executors.newFixedThreadPool(properties.getThreads());

    for (int i = 0; i < properties.getThreads(); i++) {
      executor.submit(this::generateTransactions);
    }

    executor.shutdown();

    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException ex) {
      log.error("Генерация транзакций прервана", ex);
      Thread.currentThread().interrupt();
    }
  }

  private void generateTransactions() {

    for (int i = 0; i < properties.getTransactionsPerThread(); i++) {
      TransactionDto transaction = createRandomTransaction();
      kafkaTemplate.send(properties.getTopic(), transaction.clientId().toString(), transaction);
      log.info(
          "Отправлена транзакция id={}, type={}, status={}",
          transaction.transactionId(),
          transaction.type(),
          transaction.status());
      sleepRandom();
    }
  }

  private TransactionDto createRandomTransaction() {
    TransactionType type = randomTransactionType();

    String fromAccount = (type == TransactionType.DEPOSIT) ? null : randomAccountNumber();
    String toAccount = (type == TransactionType.PURCHASE) ? null : randomAccountNumber();

    TransactionStatus status =
        random.nextInt(10) < 9 ? TransactionStatus.SUCCESS : TransactionStatus.FAILED;

    UUID clientId = clientPool.get(random.nextInt(clientPool.size()));

    return new TransactionDto(
        UUID.randomUUID(),
        clientId,
        fromAccount,
        toAccount,
        type,
        BigDecimal.valueOf(random.nextInt(1000) + 1L),
        LocalDateTime.now(),
        status,
        status == TransactionStatus.FAILED ? "Ошибка транзакции" : null);
  }

  private TransactionType randomTransactionType() {

    TransactionType[] types = TransactionType.values();
    return types[random.nextInt(types.length)];
  }

  private String randomAccountNumber() {
    return "Card №" + RandomStringUtils.randomNumeric(16);
  }

  private void sleepRandom() {
    try {
      TimeUnit.MILLISECONDS.sleep(random.nextInt(200));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
}
