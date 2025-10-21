package ru.ignatab.mapper;


import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import ru.ignatab.dto.TransactionDto;
import ru.ignatab.model.DlqMessage;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface DlqMessageMapper {
    // из DTO в сущность (добавляем причину вручную)
    @Mapping(target = "errorReason", source = "reason")
    DlqMessage toEntity(TransactionDto dto, String reason);

    // из сущности обратно в DTO
    TransactionDto toDto(DlqMessage entity);

}
