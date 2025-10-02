package ru.ignatab.config;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


/**
 * Конфигурационный класс для определения параметров генерации транзакций
 */
@Configuration
@ConfigurationProperties(prefix = "generator")
@Getter
@Setter
public class GeneratorProperties {

    /**
     * название топика kafka
     */
    private String topic;
    /**
     * Количество потоков в пуле генерации транзакций
     */
    private int threads;
    /**
     * Количество транзакций созданных в одном пуле
     */
    private int transactionsPerThread;
    /**
     * Время между созданием транзакций
     */
    private int maxSleepMs;
    /**
     * Количество клиентов, совершающих транзакции
     */
    private int clients;

}
