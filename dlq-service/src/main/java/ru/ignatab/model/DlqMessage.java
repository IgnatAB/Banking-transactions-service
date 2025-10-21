package ru.ignatab.model;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.enums.TransactionType;

@Entity
@Table(name = "dlq_messages")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID transactionId;

    private UUID clientId;
    private String fromAccount;
    private String toAccount;

    @Enumerated(EnumType.STRING)
    private TransactionType type;

    private BigDecimal amount;
    private LocalDateTime createdAt;

    @Enumerated(EnumType.STRING)
    private TransactionStatus status;

    @Column(name = "error_reason")
    private String errorReason;
}
