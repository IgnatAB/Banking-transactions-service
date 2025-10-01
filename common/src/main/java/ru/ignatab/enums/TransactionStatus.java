package ru.ignatab.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Статусы транзакции
 */
@Getter
@AllArgsConstructor
public enum TransactionStatus {

    SUCCESS ("транзакция выполнена"),
    FAILED ("транзакция не выполнена");

    private final String description;

}