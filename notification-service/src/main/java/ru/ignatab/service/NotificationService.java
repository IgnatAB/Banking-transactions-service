package ru.ignatab.service;

import ru.ignatab.dto.TransactionDto;

public interface NotificationService {

    void sendApprovedNotification(TransactionDto transaction);

    void sendRejectedNotification(TransactionDto transaction);

}
