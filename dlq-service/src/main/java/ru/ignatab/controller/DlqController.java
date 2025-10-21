package ru.ignatab.controller;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.ignatab.model.DlqMessage;
import ru.ignatab.repository.DlqMessageRepository;
import ru.ignatab.service.DlqService;

@RestController
@RequestMapping("/api/dlq")
@RequiredArgsConstructor
public class DlqController {
    private final DlqMessageRepository messageRepository;
    private final DlqService dlqService;

    @GetMapping("/all")
    public List<DlqMessage> getAll() {
        return messageRepository.findAll();
    }

    @PostMapping("/retry/{id}")
    public String retry(@PathVariable("id") Long id) {
        dlqService.retryMessage(id);
        return "Retried message " + id;
    }

    @DeleteMapping("/{id}")
    public String delete(@PathVariable("id") Long id) {
        messageRepository.deleteById(id);
        return "Deleted message " + id;
    }
}