package ru.ignatab.service.impl;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.mapper.DlqMessageMapper;
import ru.ignatab.model.DlqMessage;
import ru.ignatab.repository.DlqMessageRepository;
import ru.ignatab.service.DlqService;

@Slf4j
@Service
@RequiredArgsConstructor
public class DlqServiceImpl implements DlqService {

  private final DlqMessageRepository dlqMessageRepository;
  private final DlqMessageMapper dlqMessageMapper;
  private final KafkaTemplate<String, TransactionDto> kafkaTemplate;

  private static final String RETRY_TOPIC = "transactions";

  public void saveToDatabase(TransactionDto dto, String reason) {
    DlqMessage entity = dlqMessageMapper.toEntity(dto, reason);
    dlqMessageRepository.save(entity);
    log.info("[DLQ] Сообщение сохранено в БД: {}", entity);
  }

  public boolean retryMessage(Long id) {
    Optional<DlqMessage> optional = dlqMessageRepository.findById(id);
    if (optional.isEmpty()) {
      log.warn("[DLQ] Сообщение с id={} не найдено", id);
      return false;
    }

    DlqMessage entity = optional.get();
    TransactionDto dto = dlqMessageMapper.toDto(entity);

    try {
      kafkaTemplate.send(RETRY_TOPIC, dto.transactionId().toString(), dto);
      dlqMessageRepository.delete(entity);
      log.info("[DLQ] Сообщение {} успешно ретраено в топик '{}'", id, RETRY_TOPIC);
      return true;
    } catch (Exception e) {
      log.error("[DLQ] Ошибка при ретрае сообщения {}: {}", id, e.getMessage(), e);
      return false;
    }
  }
}