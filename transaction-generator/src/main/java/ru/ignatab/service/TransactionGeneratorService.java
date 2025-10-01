package ru.ignatab.service;

/**
 * Сервис для генерации транзакций
 * <p>
 * Создает транзакции в многопоточном режиме
 */
public interface TransactionGeneratorService {

    /**
     * Метод начинает генерацию транзакций
     */
    void startGenerating();
}
