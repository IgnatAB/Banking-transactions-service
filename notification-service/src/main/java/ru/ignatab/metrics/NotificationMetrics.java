package ru.ignatab.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class NotificationMetrics {

  private final Counter approvedCounter;
  private final Counter rejectedCounter;
  private final Counter dlqCounter;

  public NotificationMetrics(MeterRegistry registry) {
    this.approvedCounter =
        Counter.builder("notification_approved_total")
            .description("Количество успешных уведомлений")
            .register(registry);

    this.rejectedCounter =
        Counter.builder("notification_rejected_total")
            .description("Количество отклоненных уведомлений")
            .register(registry);

    this.dlqCounter =
        Counter.builder("notification_dlq_total")
            .description("Количество не обработанных уведомлений")
            .register(registry);
  }

  public void incrementApproved() {
    approvedCounter.increment();
  }

  public void incrementRejected() {
    rejectedCounter.increment();
  }

  public void incrementDlq() {
    dlqCounter.increment();
  }
}
