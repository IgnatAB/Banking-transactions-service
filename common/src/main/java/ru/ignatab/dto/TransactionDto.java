package ru.ignatab.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.enums.TransactionType;

/** Dto для генерации транзакций
* В многопоточном режиме создаются транзакции разных типов и передаются в кафку.
 */
public record TransactionDto(
    /**
     * Уникальный Id транзации
     */
 UUID transactionId,
    /**
     * Уникальный номер клиента
     */
 UUID clientId,
    /**
     * Номер счета, с которого сделана транзакция (отправителя), может быть null, при DEPOSIT
     */
 String fromAccount,
    /**
     * Номер счета, на который сделана транзакция (получателя), может быть null, при PURCHASE
     */
 String toAccount,
    /**
     * Вид операции, передает одно из значений {@link TransactionType}
     */
 TransactionType type,
    /**
     * Сумма транзакции
     */
 BigDecimal amount,
    /**
     * Время транзакции
     */
 LocalDateTime timestamp,
    /**
     * Результат транзакции, передает одно из значений {@link TransactionStatus}
     */
 TransactionStatus status,
    /**
     * Сообщение в случае ошибки операции
     */
 String errorMessage

) {}
