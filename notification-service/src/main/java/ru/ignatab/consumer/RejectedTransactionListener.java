package ru.ignatab.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.service.NotificationService;

@Slf4j
@Component
@RequiredArgsConstructor
public class RejectedTransactionListener {
    private final NotificationService notificationService;

    @KafkaListener(topics = "${topics.rejected}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenRejected(TransactionDto transaction) {

        log.info("Получено сообщение из rejected-transactions: {}", transaction);
        notificationService.sendRejectedNotification(transaction);
    }
}
