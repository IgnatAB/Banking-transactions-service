package ru.ignatab.service;

import ru.ignatab.dto.TransactionDto;

public interface DlqService {
  void saveToDatabase(TransactionDto dto, String reason);

  boolean retryMessage(Long id);
}
