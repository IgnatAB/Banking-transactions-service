package ru.ignatab.mapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.enums.TransactionStatus;
import ru.ignatab.enums.TransactionType;
import ru.ignatab.model.Transaction;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-10-07T18:29:55+0300",
    comments = "version: 1.5.5.Final, compiler: javac, environment: Java 17.0.14 (Amazon.com Inc.)"
)
@Component
public class TransactionMapperImpl implements TransactionMapper {

    @Override
    public Transaction toEntity(TransactionDto transactionDto) {
        if ( transactionDto == null ) {
            return null;
        }

        Transaction.TransactionBuilder transaction = Transaction.builder();

        transaction.transactionId( transactionDto.transactionId() );
        transaction.clientId( transactionDto.clientId() );
        transaction.fromAccount( transactionDto.fromAccount() );
        transaction.toAccount( transactionDto.toAccount() );
        transaction.type( transactionDto.type() );
        transaction.amount( transactionDto.amount() );
        transaction.createdAt( transactionDto.createdAt() );
        transaction.status( transactionDto.status() );

        return transaction.build();
    }

    @Override
    public TransactionDto toDto(Transaction entity) {
        if ( entity == null ) {
            return null;
        }

        UUID transactionId = null;
        UUID clientId = null;
        String fromAccount = null;
        String toAccount = null;
        TransactionType type = null;
        BigDecimal amount = null;
        LocalDateTime createdAt = null;
        TransactionStatus status = null;
        String errorMessage = null;

        TransactionDto transactionDto = new TransactionDto( transactionId, clientId, fromAccount, toAccount, type, amount, createdAt, status, errorMessage );

        return transactionDto;
    }
}
