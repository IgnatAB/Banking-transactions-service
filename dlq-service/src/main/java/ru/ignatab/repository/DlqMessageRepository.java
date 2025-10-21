package ru.ignatab.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.ignatab.model.DlqMessage;

public interface DlqMessageRepository extends JpaRepository<DlqMessage, Long> {}
