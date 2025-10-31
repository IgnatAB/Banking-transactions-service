package ru.ignatab.service;

import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.ignatab.config.GeneratorProperties;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.enums.TransactionType;

/**
 * Сервис генерации и отправки случайных транзакций в Kafka.
 *
 * <p>Основная задача — имитация потока транзакций с помощью нескольких потоков исполнения. Каждая
 * транзакция публикуется в Kafka-топик, указанный в {@link GeneratorProperties}.
 *
 * <p>В процессе генерации используется MDC (Mapped Diagnostic Context), чтобы добавить в каждый лог
 * уникальные идентификаторы {@code txId} и {@code clientId}. Это позволяет легко отслеживать
 * конкретную транзакцию в логах и в ELK-стеке.
 *
 * <p>Пример записи лога:
 *
 * <pre>
 * {
 *   "service": "transaction-generator",
 *   "level": "INFO",
 *   "message": "Отправлена транзакция id=..., type=TRANSFER, status=SUCCESS",
 *   "txId": "a14033e4-7272-4db3-9b04-81e64adf71a5",
 *   "clientId": "54cc1be2-fa4b-4dfb-8bde-e7941affb42a"
 * }
 * </pre>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionGeneratorServiceImpl implements TransactionGeneratorService {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final GeneratorProperties properties;
  private final Random random = new Random();
  private List<UUID> clientPool;

  /**
   * Создаёт пул случайных клиентов, доступных для генерации транзакций.
   * Вызывается один раз при старте приложения.
   */
  @PostConstruct
  public void initClientPool() {

    List<UUID> pool = new ArrayList<>();
    for (int i = 0; i < properties.getClients(); i++) {
      pool.add(UUID.randomUUID());
    }
    this.clientPool = Collections.unmodifiableList(pool);
  }

  /**
   * Запускает многопоточную генерацию транзакций.
   * Каждый поток создаёт и отправляет заданное количество сообщений в Kafka.
   */
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

  /**
   * Генерирует и отправляет несколько транзакций подряд.
   * Для каждой транзакции формирует MDC-контекст с {@code txId} и {@code clientId}.
   */
  private void generateTransactions() {

    for (int i = 0; i < properties.getTransactionsPerThread(); i++) {
      TransactionDto transaction = createRandomTransaction();

      MDC.put("txId", transaction.transactionId().toString());
      MDC.put("clientId", transaction.clientId().toString());
      try{
      kafkaTemplate.send(properties.getTopic(), transaction.clientId().toString(), transaction);
      log.info(
          "Отправлена транзакция id={}, type={}, status={}",
          transaction.transactionId(),
          transaction.type(),
          transaction.status());
      } catch (Exception e) {
        log.error("Ошибка при отправке транзакции {}", transaction.transactionId(), e);
      } finally {
        MDC.clear();
      }
      sleepRandom();
    }
  }

  /**
   * Создаёт случайную транзакцию с произвольными параметрами.
   */
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
