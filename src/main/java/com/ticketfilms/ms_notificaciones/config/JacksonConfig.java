package com.ticketfilms.ms_notificaciones.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Spring Boot 4.x ya no autoconfigura ObjectMapper solo con jackson-databind
// en el classpath. Lo declaramos explícitamente para que esté disponible
// en el contexto y pueda inyectarse en los listeners.
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Soporte para LocalDateTime (usado en BoletoCompradoEvent.fechaHoraFuncion)
        mapper.registerModule(new JavaTimeModule());
        // Serializar fechas como ISO-8601 en lugar de timestamps numéricos
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Varios servicios leen el mismo tópico (ej. compra.fallida) y cada uno
        // usa solo los campos que le sirven: los demás se ignoran, no son error.
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }
}
