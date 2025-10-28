package ru.ignatab.mapper;


import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.model.DlqMessage;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface DlqMessageMapper {

    @Mapping(target = "errorReason", source = "reason")
    DlqMessage toEntity(TransactionDto dto, String reason);

    TransactionDto toDto(DlqMessage entity);

}
