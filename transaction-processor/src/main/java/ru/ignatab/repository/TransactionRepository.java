package ru.ignatab.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.ignatab.model.Transaction;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {}
