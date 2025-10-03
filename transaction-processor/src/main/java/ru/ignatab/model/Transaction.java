package ru.ignatab.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.enums.TransactionType;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Table(name = "transactions")
public class Transaction {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(nullable = false, unique = true)
  private UUID transactionId;

  @Column(nullable = false)
  private UUID clientId;

  @Column(nullable = false)
  private String fromAccount;

  @Column(nullable = false)
  private String toAccount;

  @Enumerated(EnumType.STRING)
  private TransactionType type;

  @Column(nullable = false, precision = 19, scale = 2)
  private BigDecimal amount;

  @Column(nullable = false)
  private LocalDateTime createdAt;

  @Enumerated(EnumType.STRING)
  private TransactionStatus status;
}
