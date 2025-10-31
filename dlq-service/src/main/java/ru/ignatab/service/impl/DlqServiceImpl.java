package ru.ignatab.service.impl;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.mapper.DlqMessageMapper;
import ru.ignatab.model.DlqMessage;
import ru.ignatab.repository.DlqMessageRepository;
import ru.ignatab.service.DlqService;

/**
 * Реализация сервиса для работы с "мертвыми" сообщениями (DLQ — Dead Letter Queue). Отвечает за
 * сохранение сообщений, попавших в DLQ, и их повторную отправку (retry) в основной топик.
 *
 * <p>В логировании используется MDC для добавления контекста, чтобы упростить поиск сообщений по
 * идентификатору транзакции или DLQ-записи в ELK.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DlqServiceImpl implements DlqService {

  private final DlqMessageRepository dlqMessageRepository;
  private final DlqMessageMapper dlqMessageMapper;
  private final KafkaTemplate<String, TransactionDto> kafkaTemplate;

  private static final String RETRY_TOPIC = "transactions";

  /**
   * Сохраняет сообщение в базу данных DLQ с указанием причины ошибки.
   *
   * @param dto объект транзакции
   * @param reason причина, по которой сообщение попало в DLQ
   */
  public void saveToDatabase(TransactionDto dto, String reason) {
    MDC.put("transactionId", dto.transactionId().toString());
    try {
      DlqMessage entity = dlqMessageMapper.toEntity(dto, reason);
      dlqMessageRepository.save(entity);
      MDC.put("dlqMessageId", String.valueOf(entity.getId()));
      log.info("[DLQ] Сообщение сохранено в БД: {}", entity);
    } finally {
      MDC.clear();
    }
  }

  /**
   * Повторно отправляет сообщение из DLQ в основной Kafka-топик.
   *
   * @param id идентификатор DLQ-записи
   * @return true, если сообщение успешно ретраено, иначе false
   */
  public boolean retryMessage(Long id) {
    MDC.put("dlqMessageId", String.valueOf(id));
    try {
      Optional<DlqMessage> optional = dlqMessageRepository.findById(id);
      if (optional.isEmpty()) {
        log.warn("[DLQ] Сообщение с id={} не найдено", id);
        return false;
      }

      DlqMessage entity = optional.get();
      TransactionDto dto = dlqMessageMapper.toDto(entity);
      MDC.put("transactionId", dto.transactionId().toString());

      kafkaTemplate.send(RETRY_TOPIC, dto.transactionId().toString(), dto);
      dlqMessageRepository.delete(entity);
      log.info("[DLQ] Сообщение {} успешно ретраено в топик '{}'", id, RETRY_TOPIC);
      return true;
    } catch (Exception e) {
      log.error("[DLQ] Ошибка при ретрае сообщения {}: {}", id, e.getMessage(), e);
      return false;

    } finally {
      MDC.clear();
    }
  }
}
