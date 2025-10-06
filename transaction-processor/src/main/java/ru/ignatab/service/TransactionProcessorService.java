package ru.ignatab.service;

import ru.ignatab.dto.TransactionDto;

public interface TransactionProcessorService {

  /**
   * Основной метод обработки транзакции
   *
   * @param dto входящая транзакция
   */
  void processTransaction(TransactionDto dto);

  /**
   * Отправка в топик отклонённых транзакций (бизнес-ошибки)
   *
   * @param dto входящая транзакция
   * @param errorMessage описание ошибки
   */
  void sendToRejected(TransactionDto dto, String errorMessage);

  /**
   * Отправка сообщения в DLQ (ошибки не связанные с бизнес-логикой)
   *
   * @param dto транзакция, не подлежащая обработке
   * @param errorMessage описание ошибки
   */
  void sendToDlq(TransactionDto dto, String errorMessage);
}
