package ru.ignatab.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class TransactionProcessorMetrics {
    private final Counter approvedTransactionCounter;
    private final Counter rejectedTransactionCounter;
    private final Counter dlqTransactionCounter;
    private final Counter dbAddCounter;

    public TransactionProcessorMetrics(MeterRegistry registry) {
        this.approvedTransactionCounter =
            Counter.builder("transaction_approved_total")
                .description("Количество успешных уведомлений")
                .register(registry);

        this.rejectedTransactionCounter =
            Counter.builder("transaction_rejected_total")
                .description("Количество отклоненных уведомлений")
                .register(registry);

        this.dlqTransactionCounter =
            Counter.builder("transaction_dlq_total")
                .description("Количество не обработанных уведомлений")
                .register(registry);

        this.dbAddCounter = Counter.builder("db_added_transactions")
            .description("Количество добавленных транзакций в БД")
            .register(registry);
    }

    public void incrementApproved() {
        approvedTransactionCounter.increment();
    }

    public void incrementRejected() {
        rejectedTransactionCounter.increment();
    }

    public void incrementDlq() {
        dlqTransactionCounter.increment();
    }

    public void incrementDb(){
        dbAddCounter.increment();
    }
}

