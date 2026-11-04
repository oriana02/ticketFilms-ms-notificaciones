package com.ticketfilms.ms_notificaciones.listener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.ticketfilms.ms_notificaciones.config.RabbitTopology;
import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;
import com.ticketfilms.ms_notificaciones.exception.MensajeInvalidoException;
import com.ticketfilms.ms_notificaciones.service.EmailService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Los métodos recibir* son la capa de mensajería: reciben y delegan el ACK/reintentos
// a ProcesadorAck. Los métodos consumir* validan el mensaje y envían el correo.
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificacionesListener {

    private final ObjectMapper objectMapper;
    private final EmailService emailService;
    private final ProcesadorAck procesadorAck;

    // ---------- Recepción (RabbitMQ) ----------
    @RabbitListener(queues = RabbitTopology.Q_BOLETO_COMPRADO)
    public void recibirBoletoComprado(Message mensaje, Channel canal) throws IOException {
        procesadorAck.procesar(mensaje, canal, () -> consumirBoletoComprado(cuerpo(mensaje)));
    }

    @RabbitListener(queues = RabbitTopology.Q_COMPRA_FALLIDA_NOTIFICACIONES)
    public void recibirCompraFallida(Message mensaje, Channel canal) throws IOException {
        procesadorAck.procesar(mensaje, canal, () -> consumirCompraFallida(cuerpo(mensaje)));
    }

    @RabbitListener(queues = RabbitTopology.Q_RESERVA_EXPIRADA)
    public void recibirReservaExpirada(Message mensaje, Channel canal) throws IOException {
        procesadorAck.procesar(mensaje, canal, () -> consumirReservaExpirada(cuerpo(mensaje)));
    }

    // ---------- Procesamiento ----------
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

    public void consumirCompraFallida(String mensaje) {
        CompraFallidaEvent evento = leerEvento(mensaje, CompraFallidaEvent.class);
        exigir(evento.motivo(), "motivo");
        // Esta cola es una copia del evento; ms-asientos recibe otra para liberar asientos.
        // Sin email no hay a quién avisar, pero eso no es un mensaje inválido.
        if (evento.emailUsuario() == null || evento.emailUsuario().isBlank()) {
            log.warn("Compra fallida sin email, se omite el correo - boleto {}", evento.codigoBoleto());
            return;
        }
        emailService.enviarCompraFallida(evento);
        log.info("Correo de compra fallida enviado a {} - boleto {} - motivo: {}",
                evento.emailUsuario(), evento.codigoBoleto(), evento.motivo());
    }

    public void consumirReservaExpirada(String mensaje) {
        ReservaExpiradaEvent evento = leerEvento(mensaje, ReservaExpiradaEvent.class);
        exigir(evento.emailUsuario(), "emailUsuario");
        exigirPresente(evento.funcionId(), "funcionId");
        if (evento.asientos() == null || evento.asientos().isEmpty()) {
            throw new MensajeInvalidoException("Falta el campo obligatorio: asientos");
        }
        emailService.enviarReservaExpirada(evento);
        log.info("Correo de reserva expirada enviado a {} - función {}",
                evento.emailUsuario(), evento.funcionId());
    }

    private String cuerpo(Message mensaje) {
        return new String(mensaje.getBody(), StandardCharsets.UTF_8);
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

    // Campo obligatorio faltante = error definitivo (va directo a la DLQ, sin reintentos)
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
