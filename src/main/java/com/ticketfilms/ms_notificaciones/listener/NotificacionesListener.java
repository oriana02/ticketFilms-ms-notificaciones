package com.ticketfilms.ms_notificaciones.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;

import lombok.RequiredArgsConstructor;
import tools.jackson.databind.json.JsonMapper;

// Escucha los tres tópicos y avisa al usuario. Si un mensaje viene mal formado
// solo se registra el error: no se lanza la excepción para que Kafka no
// reintente el mismo mensaje una y otra vez.
@Component
@RequiredArgsConstructor
public class NotificacionesListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacionesListener.class);

    private final JsonMapper jsonMapper;

    @KafkaListener(topics = "boleto.comprado")
    public void alComprarBoleto(String mensaje) {
        try {
            BoletoCompradoEvent evento = jsonMapper.readValue(mensaje, BoletoCompradoEvent.class);
            // AQUÍ irá el correo real con el QR del boleto
            log.info("[SIMULADO] Correo de confirmación a {} — boleto {} — {} — asientos {} — total {}",
                    evento.emailUsuario(), evento.codigoBoleto(), evento.tituloEvento(),
                    evento.asientos(), evento.precioTotal());
        } catch (Exception e) {
            log.error("Mensaje inválido en boleto.comprado: {} — motivo={}", mensaje, e.getMessage());
        }
    }

    @KafkaListener(topics = "compra.fallida")
    public void alFallarCompra(String mensaje) {
        try {
            CompraFallidaEvent evento = jsonMapper.readValue(mensaje, CompraFallidaEvent.class);
            // AQUÍ irá el correo real avisando que la compra no se pudo completar
            log.info("[SIMULADO] Aviso de compra fallida a {} — boleto {} — motivo: {}",
                    evento.emailUsuario(), evento.codigoBoleto(), evento.motivo());
        } catch (Exception e) {
            log.error("Mensaje inválido en compra.fallida: {} — motivo={}", mensaje, e.getMessage());
        }
    }

    @KafkaListener(topics = "reserva.expirada")
    public void alExpirarReserva(String mensaje) {
        try {
            ReservaExpiradaEvent evento = jsonMapper.readValue(mensaje, ReservaExpiradaEvent.class);
            // AQUÍ irá el correo real avisando que la reserva venció
            log.info("[SIMULADO] Aviso de reserva vencida a {} — función {} — asientos {}",
                    evento.emailUsuario(), evento.funcionId(), evento.asientos());
        } catch (Exception e) {
            log.error("Mensaje inválido en reserva.expirada: {} — motivo={}", mensaje, e.getMessage());
        }
    }
}
