package com.ticketfilms.ms_notificaciones.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.ms_notificaciones.config.KafkaTopics;
import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;
import com.ticketfilms.ms_notificaciones.exception.MensajeInvalidoException;
import com.ticketfilms.ms_notificaciones.service.EmailService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificacionesListener {

    private final ObjectMapper objectMapper;
    private final EmailService emailService;

    @KafkaListener(topics = KafkaTopics.BOLETO_COMPRADO, groupId = "ms-notificaciones")
    public void consumirBoletoComprado(String mensaje) {
        BoletoCompradoEvent evento = leerEvento(mensaje, BoletoCompradoEvent.class);
        exigir(evento.emailUsuario(), "emailUsuario");
        exigir(evento.codigoBoleto(), "codigoBoleto");
        exigir(evento.tituloEvento(), "tituloEvento");
        exigirPresente(evento.fechaHoraFuncion(), "fechaHoraFuncion");
        exigirPresente(evento.precioTotal(), "precioTotal");
        if (evento.asientos() == null || evento.asientos().isEmpty()) {
            throw new MensajeInvalidoException("Falta el campo obligatorio: asientos");
        }
        emailService.enviarConfirmacion(evento);
        log.info("Correo de confirmación enviado a {} - boleto {}", evento.emailUsuario(), evento.codigoBoleto());
    }

    @KafkaListener(topics = KafkaTopics.COMPRA_FALLIDA, groupId = "ms-notificaciones")
    public void consumirCompraFallida(String mensaje) {
        CompraFallidaEvent evento = leerEvento(mensaje, CompraFallidaEvent.class);
        exigir(evento.emailUsuario(), "emailUsuario");
        exigir(evento.motivo(), "motivo");
        emailService.enviarCompraFallida(evento);
        log.info("Correo de compra fallida enviado a {} - boleto {} - motivo: {}",
                evento.emailUsuario(), evento.motivo());
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

    // Campo obligatorio faltante = error definitivo (va directo al DLT, sin reintentos)
    private void exigir(String valor, String campo) {
        if (valor == null || valor.isBlank()) {
            throw new MensajeInvalidoException("Falta el campo obligatorio: " + campo);
        }
    }

    private void exigirPresente(Object valor, String campo) {
        if (valor == null) {
            throw new MensajeInvalidoException("Falta el campo obligatorio: " + campo);
        }
    }
}
