package com.ticketfilms.ms_notificaciones.listener;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;
import com.ticketfilms.ms_notificaciones.exception.MensajeInvalidoException;
import com.ticketfilms.ms_notificaciones.service.EmailService;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificacionesListenerTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private EmailService emailService;

    @InjectMocks
    private NotificacionesListener listener;

    // ============================================================
    // boleto.comprado
    // ============================================================
    @Test
    void consumirBoletoComprado_mensajeValido_enviaCorreo() throws Exception {
        BoletoCompradoEvent evento = new BoletoCompradoEvent(
                "TF-AB12-CD34",
                "a@b.cl",
                "Dune 3",
                LocalDateTime.of(2026, 10, 18, 20, 0),
                new BigDecimal("11000"),
                List.of("F7", "F8"));

        when(objectMapper.readValue(anyString(), eq(BoletoCompradoEvent.class)))
                .thenReturn(evento);

        assertDoesNotThrow(() -> listener.consumirBoletoComprado("{\"cualquier\":\"json\"}"));
        verify(emailService).enviarConfirmacion(evento);
    }

    @Test
    void consumirBoletoComprado_mensajeInvalido_lanzaMensajeInvalidoException() throws Exception {
        when(objectMapper.readValue(anyString(), eq(BoletoCompradoEvent.class)))
                .thenThrow(new JsonParseException(null, "JSON inválido"));

        assertThrows(MensajeInvalidoException.class,
                () -> listener.consumirBoletoComprado("esto-no-es-json"));
        verifyNoInteractions(emailService);
    }

    @Test
    void consumirBoletoComprado_sinEmail_lanzaMensajeInvalidoException() throws Exception {
        BoletoCompradoEvent incompleto = new BoletoCompradoEvent(
                "TF-AB12-CD34",
                null,
                "Dune 3",
                LocalDateTime.of(2026, 10, 18, 20, 0),
                new BigDecimal("11000"),
                List.of("F7"));

        when(objectMapper.readValue(anyString(), eq(BoletoCompradoEvent.class)))
                .thenReturn(incompleto);

        assertThrows(MensajeInvalidoException.class,
                () -> listener.consumirBoletoComprado("{\"sin\":\"email\"}"));
        verifyNoInteractions(emailService);
    }

    // ============================================================
    // compra.fallida
    // ============================================================
    @Test
    void consumirCompraFallida_mensajeValido_procesaSinError() throws Exception {
        CompraFallidaEvent evento = new CompraFallidaEvent(
                "TF-ZZ99-XX11",
                "a@b.cl",
                "La reserva expiró");

        when(objectMapper.readValue(anyString(), eq(CompraFallidaEvent.class)))
                .thenReturn(evento);

        assertDoesNotThrow(() -> listener.consumirCompraFallida("{\"x\":\"y\"}"));
    }

    @Test
    void consumirCompraFallida_mensajeInvalido_lanzaMensajeInvalidoException() throws Exception {
        when(objectMapper.readValue(anyString(), eq(CompraFallidaEvent.class)))
                .thenThrow(new JsonParseException(null, "JSON inválido"));

        assertThrows(MensajeInvalidoException.class,
                () -> listener.consumirCompraFallida("no-json"));
    }

    // ============================================================
    // reserva.expirada
    // ============================================================
    @Test
    void consumirReservaExpirada_mensajeValido_procesaSinError() throws Exception {
        ReservaExpiradaEvent evento = new ReservaExpiradaEvent(
                5L,
                List.of("F7", "F8"),
                "a@b.cl");

        when(objectMapper.readValue(anyString(), eq(ReservaExpiradaEvent.class)))
                .thenReturn(evento);

        assertDoesNotThrow(() -> listener.consumirReservaExpirada("{\"x\":\"y\"}"));
    }

    @Test
    void consumirReservaExpirada_mensajeInvalido_lanzaMensajeInvalidoException() throws Exception {
        when(objectMapper.readValue(anyString(), eq(ReservaExpiradaEvent.class)))
                .thenThrow(new JsonParseException(null, "JSON inválido"));

        assertThrows(MensajeInvalidoException.class,
                () -> listener.consumirReservaExpirada("mal-formado"));
    }
}
