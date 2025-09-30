# Banking-transactions-service
I am writing an example of using the Kafka message broker for fintech

Цель
Создать мини-экосистему из микросервисов, которая имитирует работу банковской системы с использованием Kafka для обмена сообщениями.
Это позволит показать:
•	 работу Kafka Producer / Consumer
•	 Consumer Groups и балансировку нагрузки
•	 многопоточность при генерации сообщений
•	 обработку событий с последующими шагами (workflow)
•	 надежность и fault tolerance (ретраи, dead-letter-topic)
________________________________________
## Архитектура
1. Transaction Generator (Producer-сервис)
•	Симулирует клиентов, которые выполняют транзакции:
o	Пополнение счета
o	Перевод денег
o	Оплата покупки
•	Работает в многопоточке → одновременно генерирует N транзакций.
•	Отправляет события в Kafka topic transactions.
________________________________________
2. Transaction Processor (Consumer-сервис)
•	Подписан на topic transactions.
•	Проверяет:
o	наличие счёта,
o	достаточно ли средств,
o	корректность данных.
•	Если всё хорошо → записывает результат в approved-transactions.
•	Если ошибка → отправляет в rejected-transactions.
________________________________________
3. Balance Service (Consumer group)
•	Несколько инстансов читают approved-transactions.
•	Обновляют баланс счетов в базе (PostgreSQL).
•	Здесь будет демонстрация consumer group → сообщения распределяются между несколькими инстансами.
________________________________________
4. Notification Service (Fan-out)
•	Подписан на оба топика: approved-transactions и rejected-transactions.
•	Отправляет уведомления (например, в консоль или “email-эмулятор”).
•	Показывает, как одно событие можно обрабатывать несколькими сервисами параллельно.
________________________________________
5. Dead Letter Queue (DLQ)
•	Если consumer не может обработать сообщение (например, ошибка сериализации) → оно попадает в отдельный топик transactions-dlq.
•	Будет отдельный consumer для анализа "плохих сообщений".
________________________________________
## Технологический стек
•	Java 17
•	Spring Boot 3
•	Spring Kafka
•	PostgreSQL (для хранения счетов и балансов)
•	Docker + Docker Compose (для Kafka и БД)
•	Testcontainers (для интеграционных тестов)
•	Lombok, MapStruct, JUnit5, Mockito
