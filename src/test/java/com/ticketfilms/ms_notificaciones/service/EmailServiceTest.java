package com.ticketfilms.ms_notificaciones.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mail.MailSendException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.util.ReflectionTestUtils;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

@ExtendWith(MockitoExtension.class)
class EmailServiceTest {

    private static final String REMITENTE = "no-reply@test.cl";

    @Mock
    private JavaMailSender mailSender;

    @Mock
    private QrService qrService;

    @Mock
    private TemplateEngine templateEngine; // NUEVO MOCK

    private EmailService emailService;

    @BeforeEach
    void setUp() {
        // Agregamos templateEngine al constructor
        emailService = new EmailService(mailSender, qrService, templateEngine);
        ReflectionTestUtils.setField(emailService, "remitente", REMITENTE);
    }

    // Un MimeMessage real en memoria: así se puede revisar lo que EmailService arma
    private void prepararMensaje() {
        when(mailSender.createMimeMessage())
                .thenReturn(new MimeMessage(Session.getInstance(new Properties())));
    }

    private MimeMessage mensajeEnviado() {
        ArgumentCaptor<MimeMessage> captor = ArgumentCaptor.forClass(MimeMessage.class);
        verify(mailSender).send(captor.capture());
        return captor.getValue();
    }

    private String destinatario(MimeMessage mensaje) throws Exception {
        return mensaje.getRecipients(Message.RecipientType.TO)[0].toString();
    }

    private BoletoCompradoEvent boleto(String titulo) {
        return new BoletoCompradoEvent(
                "TF-AB12-CD34", "a@b.cl", titulo,
                LocalDateTime.of(2026, 10, 18, 20, 0),
                new BigDecimal("11000"), List.of("F7", "F8"));
    }

    // ============================================================
    // boleto.comprado
    // ============================================================
    @Test
    void enviarConfirmacion_armaCorreoConDatosDelBoleto() throws Exception {
        prepararMensaje();
        when(qrService.generarPng("TF-AB12-CD34")).thenReturn(new byte[]{1, 2, 3});

        // Simulamos que Thymeleaf devuelve un HTML básico
        when(templateEngine.process(eq("correo-boleto-comprado"), any(Context.class)))
                .thenReturn("<html>Correo de confirmación</html>");

        emailService.enviarConfirmacion(boleto("Dune 3"));

        MimeMessage mensaje = mensajeEnviado();

        // Verificamos los datos del correo (destinatario, remitente, asunto)
        assertEquals("a@b.cl", destinatario(mensaje));
        assertEquals(REMITENTE, mensaje.getFrom()[0].toString());
        assertTrue(mensaje.getSubject().contains("TF-AB12-CD34"));

        // Verificamos que Thymeleaf recibió los datos correctos en el Context
        ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(templateEngine).process(eq("correo-boleto-comprado"), contextCaptor.capture());
        Context context = contextCaptor.getValue();

        assertEquals("Dune 3", context.getVariable("tituloEvento"));
        assertEquals("F7, F8", context.getVariable("asientos"));
        assertEquals("18-10-2026 a las 20:00", context.getVariable("fecha"));
        assertEquals("11.000", context.getVariable("total")); // Formato es-CL
        assertEquals("TF-AB12-CD34", context.getVariable("codigoBoleto"));
    }

    // ============================================================
    // compra.fallida
    // ============================================================
    @Test
    void enviarCompraFallida_incluyeMotivoYReferencia() throws Exception {
        prepararMensaje();

        when(templateEngine.process(eq("correo-compra-fallida"), any(Context.class)))
                .thenReturn("<html>Compra fallida</html>");

        emailService.enviarCompraFallida(
                new CompraFallidaEvent("TF-ZZ99-XX11", "a@b.cl", "La reserva expiró"));

        MimeMessage mensaje = mensajeEnviado();
        assertEquals("a@b.cl", destinatario(mensaje));
        verifyNoInteractions(qrService);

        // Verificamos el Context
        ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(templateEngine).process(eq("correo-compra-fallida"), contextCaptor.capture());
        Context context = contextCaptor.getValue();

        assertEquals("La reserva expiró", context.getVariable("motivo"));
        assertEquals("TF-ZZ99-XX11", context.getVariable("referencia"));
    }

    @Test
    void enviarCompraFallida_sinMotivo_usaMensajePorDefecto() throws Exception {
        prepararMensaje();

        when(templateEngine.process(eq("correo-compra-fallida"), any(Context.class)))
                .thenReturn("<html>Compra fallida</html>");

        emailService.enviarCompraFallida(new CompraFallidaEvent(null, "a@b.cl", " "));

        // Verificamos el Context
        ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(templateEngine).process(eq("correo-compra-fallida"), contextCaptor.capture());
        Context context = contextCaptor.getValue();

        assertEquals("No se pudo completar el pago", context.getVariable("motivo"));
        assertEquals(null, context.getVariable("referencia"));
    }

    // ============================================================
    // reserva.expirada
    // ============================================================
    @Test
    void enviarReservaExpirada_incluyeAsientos() throws Exception {
        prepararMensaje();

        when(templateEngine.process(eq("correo-reserva-expirada"), any(Context.class)))
                .thenReturn("<html>Reserva expirada</html>");

        emailService.enviarReservaExpirada(
                new ReservaExpiradaEvent(5L, List.of("F7", "F8"), "a@b.cl"));

        MimeMessage mensaje = mensajeEnviado();
        assertEquals("a@b.cl", destinatario(mensaje));

        // Verificamos el Context
        ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(templateEngine).process(eq("correo-reserva-expirada"), contextCaptor.capture());
        Context context = contextCaptor.getValue();

        assertEquals("F7, F8", context.getVariable("asientos"));
        assertEquals(5L, context.getVariable("funcionId"));
    }

    // ============================================================
    // Error recuperable: el SMTP falla y la excepción sube al listener,
    // que es lo que activa los reintentos y luego el DLT
    // ============================================================
    @Test
    void enviarReservaExpirada_smtpCaido_propagaLaExcepcion() {
        prepararMensaje();

        // Simulamos que Thymeleaf genera el HTML
        when(templateEngine.process(eq("correo-reserva-expirada"), any(Context.class)))
                .thenReturn("<html>Reserva expirada</html>");

        // Simulamos la caída de SMTP
        doThrow(new MailSendException("SMTP caído")).when(mailSender).send(any(MimeMessage.class));

        assertThrows(MailSendException.class, () -> emailService.enviarReservaExpirada(
                new ReservaExpiradaEvent(5L, List.of("F7"), "a@b.cl")));
    }
}
