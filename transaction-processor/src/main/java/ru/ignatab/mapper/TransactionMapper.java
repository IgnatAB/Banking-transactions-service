package ru.ignatab.mapper;

import org.mapstruct.Mapper;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.model.Transaction;

@Mapper(componentModel = "spring")
public interface TransactionMapper {

    Transaction toEntity (TransactionDto transactionDto);

}
