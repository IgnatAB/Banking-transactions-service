package ru.ignatab.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.service.DlqService;

@Slf4j
@Service
@RequiredArgsConstructor
public class DlqListener {


    private final DlqService dlqService;
    private final KafkaTemplate<String, TransactionDto> kafkaTemplate;

    @KafkaListener(
        topics = "${topics.dlq}",
        groupId = "dlq-processor-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleDlqMessage(TransactionDto transaction) {
        log.warn("[DLQ] Получено сообщение из DLQ: {}", transaction);

        try {
            // сохраняем в БД вместе с причиной ошибки
            String errorReason = transaction.errorMessage() != null
                ? transaction.errorMessage()
                : "Неизвестная ошибка при уведомлении";
            dlqService.saveToDatabase(transaction, errorReason);

        } catch (Exception e) {
            log.error("[DLQ] Ошибка при сохранении сообщения в БД: {}", e.getMessage(), e);
        }
    }
}