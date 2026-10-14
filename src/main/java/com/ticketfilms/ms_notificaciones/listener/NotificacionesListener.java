package com.ticketfilms.ms_notificaciones.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.ms_notificaciones.config.KafkaTopics;
import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;
import com.ticketfilms.ms_notificaciones.exception.MensajeInvalidoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificacionesListener {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.BOLETO_COMPRADO, groupId = "ms-notificaciones")
    public void consumirBoletoComprado(String mensaje) {
        BoletoCompradoEvent evento = leerEvento(mensaje, BoletoCompradoEvent.class);
        log.info("[SIMULADO] Correo de confirmación a {} - boleto {} - {} - asientos {} - total {}",
                evento.emailUsuario(),
                evento.codigoBoleto(),
                evento.tituloEvento(),
                evento.asientos(),
                evento.precioTotal());
    }

    @KafkaListener(topics = KafkaTopics.COMPRA_FALLIDA, groupId = "ms-notificaciones")
    public void consumirCompraFallida(String mensaje) {
        CompraFallidaEvent evento = leerEvento(mensaje, CompraFallidaEvent.class);
        log.info("[SIMULADO] Aviso de compra fallida a {} - boleto {} - motivo: {}",
                evento.emailUsuario(),
                evento.codigoBoleto(),
                evento.motivo());
    }

    @KafkaListener(topics = KafkaTopics.RESERVA_EXPIRADA, groupId = "ms-notificaciones")
    public void consumirReservaExpirada(String mensaje) {
        ReservaExpiradaEvent evento = leerEvento(mensaje, ReservaExpiradaEvent.class);
        log.info("[SIMULADO] Aviso de reserva vencida a {} - función {} - asientos {}",
                evento.emailUsuario(),
                evento.funcionId(),
                evento.asientos());
    }

    private <T> T leerEvento(String mensaje, Class<T> tipo) {
        try {
            T evento = objectMapper.readValue(mensaje, tipo);
            if (evento == null) {
                throw new MensajeInvalidoException("El mensaje no se pudo deserializar a " + tipo.getSimpleName());
            }
            return evento;
        } catch (JsonProcessingException e) {
            throw new MensajeInvalidoException(
                    "El mensaje no es un JSON válido para " + tipo.getSimpleName(), e);
        }
    }
}
