package com.ticketfilms.ms_notificaciones.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

// Cliente de la API de gestión de RabbitMQ (puerto 15672). AMQP no permite listar
// colas ni exchanges, por eso RabbitAdminService usa esta API solo para listar.
@Configuration
public class RabbitManagementConfig {

    @Bean
    public RestClient rabbitManagementClient(
            @Value("${app.rabbit.management-url}") String url,
            @Value("${spring.rabbitmq.username}") String usuario,
            @Value("${spring.rabbitmq.password}") String clave) {
        return RestClient.builder()
                .baseUrl(url)
                .defaultHeaders(h -> h.setBasicAuth(usuario, clave))
                .build();
    }
}
