package com.ticketfilms.ms_notificaciones.dto;

import java.time.LocalDateTime;

// Un mensaje que terminó en una DLQ, con el contexto para diagnosticarlo.
public record FalloRegistrado(
        LocalDateTime recibidoEn,
        String dlq,
        String colaOrigen,
        String clase,
        String motivo,
        String intentos,
        String routingKey,
        String messageId,
        String payload) {

}
