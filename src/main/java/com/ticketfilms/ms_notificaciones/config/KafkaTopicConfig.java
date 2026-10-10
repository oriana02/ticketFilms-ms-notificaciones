package com.ticketfilms.ms_notificaciones.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Si el tópico ya existe, no pasa nada. Sirve para que ms-notificaciones
// no dé avisos cuando arranca antes que los productores.
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic boletoComprado() {
        return TopicBuilder.name("boleto.comprado").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic compraFallida() {
        return TopicBuilder.name("compra.fallida").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic reservaExpirada() {
        return TopicBuilder.name("reserva.expirada").partitions(1).replicas(1).build();
    }
}
