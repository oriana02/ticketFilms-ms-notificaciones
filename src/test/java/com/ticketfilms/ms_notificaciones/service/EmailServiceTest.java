package com.ticketfilms.ms_notificaciones.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;
import jakarta.mail.Message;
import jakarta.mail.Multipart;
import jakarta.mail.Part;
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

@ExtendWith(MockitoExtension.class)
class EmailServiceTest {

    private static final String REMITENTE = "no-reply@test.cl";

    @Mock
    private JavaMailSender mailSender;

    @Mock
    private QrService qrService;

    private EmailService emailService;

    @BeforeEach
    void setUp() {
        emailService = new EmailService(mailSender, qrService);
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

    // Junta el texto de todas las partes del correo (HTML incluido)
    private String texto(Part parte) throws Exception {
        Object contenido = parte.getContent();
        if (contenido instanceof String s) {
            return s;
        }
        if (contenido instanceof Multipart multipart) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < multipart.getCount(); i++) {
                sb.append(texto(multipart.getBodyPart(i)));
            }
            return sb.toString();
        }
        return "";
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

        emailService.enviarConfirmacion(boleto("Dune 3"));

        MimeMessage mensaje = mensajeEnviado();
        String html = texto(mensaje);
        assertEquals("a@b.cl", destinatario(mensaje));
        assertEquals(REMITENTE, mensaje.getFrom()[0].toString());
        assertTrue(mensaje.getSubject().contains("TF-AB12-CD34"));
        assertTrue(html.contains("Dune 3"));
        assertTrue(html.contains("F7, F8"));
        assertTrue(html.contains("18-10-2026 a las 20:00"));
        assertTrue(html.contains("cid:qr"));
    }

    @Test
    void enviarConfirmacion_escapaHtmlDelTitulo() throws Exception {
        prepararMensaje();
        when(qrService.generarPng(anyString())).thenReturn(new byte[]{1});

        emailService.enviarConfirmacion(boleto("<script>alert(1)</script>"));

        String html = texto(mensajeEnviado());
        assertFalse(html.contains("<script>"));
        assertTrue(html.contains("&lt;script&gt;"));
    }

    // ============================================================
    // compra.fallida
    // ============================================================
    @Test
    void enviarCompraFallida_incluyeMotivoYReferencia() throws Exception {
        prepararMensaje();

        emailService.enviarCompraFallida(
                new CompraFallidaEvent("TF-ZZ99-XX11", "a@b.cl", "La reserva expiró"));

        MimeMessage mensaje = mensajeEnviado();
        String html = texto(mensaje);
        assertEquals("a@b.cl", destinatario(mensaje));
        assertTrue(html.contains("La reserva expir"));
        assertTrue(html.contains("TF-ZZ99-XX11"));
        verifyNoInteractions(qrService);
    }

    @Test
    void enviarCompraFallida_sinMotivo_usaMensajePorDefecto() throws Exception {
        prepararMensaje();

        emailService.enviarCompraFallida(new CompraFallidaEvent(null, "a@b.cl", " "));

        String html = texto(mensajeEnviado());
        assertTrue(html.contains("No se pudo completar el pago"));
        assertFalse(html.contains("Referencia"));
    }

    // ============================================================
    // reserva.expirada
    // ============================================================
    @Test
    void enviarReservaExpirada_incluyeAsientos() throws Exception {
        prepararMensaje();

        emailService.enviarReservaExpirada(
                new ReservaExpiradaEvent(5L, List.of("F7", "F8"), "a@b.cl"));

        MimeMessage mensaje = mensajeEnviado();
        String html = texto(mensaje);
        assertEquals("a@b.cl", destinatario(mensaje));
        assertTrue(html.contains("F7, F8"));
        assertTrue(html.contains("5"));
    }

    // ============================================================
    // Error recuperable: el SMTP falla y la excepción sube al listener,
    // que es lo que activa los reintentos y luego el DLT
    // ============================================================
    @Test
    void enviarReservaExpirada_smtpCaido_propagaLaExcepcion() {
        prepararMensaje();
        doThrow(new MailSendException("SMTP caído")).when(mailSender).send(any(MimeMessage.class));

        assertThrows(MailSendException.class, () -> emailService.enviarReservaExpirada(
                new ReservaExpiradaEvent(5L, List.of("F7"), "a@b.cl")));
    }
}
