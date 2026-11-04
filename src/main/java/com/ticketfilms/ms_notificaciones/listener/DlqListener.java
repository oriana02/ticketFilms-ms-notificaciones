package com.ticketfilms.ms_notificaciones.listener;

import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.DLQ_BOLETO_COMPRADO;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.DLQ_COMPRA_FALLIDA_ASIENTOS;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.DLQ_COMPRA_FALLIDA_NOTIFICACIONES;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.DLQ_RESERVA_EXPIRADA;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;
import com.ticketfilms.ms_notificaciones.dto.FalloRegistrado;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Monitorea las 4 DLQ: deja en el log y en memoria QUÉ falló, DÓNDE, POR QUÉ y CUÁNTAS
// veces se intentó, además del mensaje original. Se desactiva con app.rabbit.dlq-listener=false
// para dejar los mensajes visibles en la consola de RabbitMQ.
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.rabbit.dlq-listener", havingValue = "true", matchIfMissing = true)
public class DlqListener {

    private final DlqRegistro registro;

    @RabbitListener(queues = {
        DLQ_BOLETO_COMPRADO,
        DLQ_COMPRA_FALLIDA_NOTIFICACIONES,
        DLQ_COMPRA_FALLIDA_ASIENTOS,
        DLQ_RESERVA_EXPIRADA})
    public void recibir(Message mensaje, Channel canal) throws IOException {
        MessageProperties p = mensaje.getMessageProperties();

        FalloRegistrado fallo = new FalloRegistrado(
                LocalDateTime.now(),
                p.getConsumerQueue(),
                primero(p, "x-error-cola-origen", "x-first-death-queue"),
                primero(p, "x-error-clase"),
                primero(p, "x-error-motivo", "x-first-death-reason"),
                primero(p, "x-error-intentos"),
                p.getReceivedRoutingKey(),
                p.getMessageId() == null ? "sin-id" : p.getMessageId(),
                new String(mensaje.getBody(), StandardCharsets.UTF_8));

        registro.agregar(fallo);
        log.error("DLQ | dlq={} | colaOrigen={} | clase={} | motivo={} | intentos={} | routingKey={} | messageId={} | payload={}",
                fallo.dlq(), fallo.colaOrigen(), fallo.clase(), fallo.motivo(), fallo.intentos(),
                fallo.routingKey(), fallo.messageId(), fallo.payload());

        canal.basicAck(p.getDeliveryTag(), false);
    }

    private String primero(MessageProperties p, String... claves) {
        for (String clave : claves) {
            Object valor = p.getHeader(clave);
            if (valor != null) {
                return String.valueOf(valor);
            }
        }
        return "desconocido";
    }
}
