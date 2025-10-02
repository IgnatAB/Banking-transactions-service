package ru.ignatab.enums;


import lombok.AllArgsConstructor;
import lombok.Getter;

/**Типы транзакций
 *
 */
@Getter
@AllArgsConstructor
public enum TransactionType {
    DEPOSIT ("пополнение счета"),
    TRANSFER ("перевод со счета на счет"),
    PURCHASE ("оплата покупок");

    private final String description;

}

