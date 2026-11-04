package com.ticketfilms.ms_notificaciones.service;

import com.ticketfilms.ms_notificaciones.dto.BoletoCompradoEvent;
import com.ticketfilms.ms_notificaciones.dto.CompraFallidaEvent;
import com.ticketfilms.ms_notificaciones.dto.ReservaExpiradaEvent;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import java.text.NumberFormat;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mail.MailPreparationException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;        
import org.thymeleaf.context.Context;        

@Service
public class EmailService {

    private static final DateTimeFormatter FORMATO_FECHA
            = DateTimeFormatter.ofPattern("dd-MM-yyyy 'a las' HH:mm");

    private final JavaMailSender mailSender;
    private final QrService qrService;
    private final TemplateEngine templateEngine; // <-- NUEVA DEPENDENCIA

    @Value("${app.mail.from}")
    private String remitente;

    // Actualizamos el constructor
    public EmailService(JavaMailSender mailSender, QrService qrService, TemplateEngine templateEngine) {
        this.mailSender = mailSender;
        this.qrService = qrService;
        this.templateEngine = templateEngine;
    }

    public void enviarConfirmacion(BoletoCompradoEvent e) {
        byte[] qr = qrService.generarPng(e.codigoBoleto());

        String fecha = e.fechaHoraFuncion().format(FORMATO_FECHA);
        String total = NumberFormat.getIntegerInstance(Locale.forLanguageTag("es-CL"))
                .format(e.precioTotal());

        // 1. Preparamos el contexto de Thymeleaf
        Context context = new Context();
        context.setVariable("tituloEvento", e.tituloEvento());
        context.setVariable("fecha", fecha);
        context.setVariable("asientos", String.join(", ", e.asientos()));
        context.setVariable("total", total);
        context.setVariable("codigoBoleto", e.codigoBoleto());

        // 2. Procesamos la plantilla HTML
        String html = templateEngine.process("correo-boleto-comprado", context);

        enviar(e.emailUsuario(), "Tu boleto " + e.codigoBoleto() + " - TicketFilms", html, qr);
    }

    public void enviarCompraFallida(CompraFallidaEvent e) {
        String motivo = (e.motivo() == null || e.motivo().isBlank())
                ? "No se pudo completar el pago"
                : e.motivo();

        String referencia = (e.codigoBoleto() == null || e.codigoBoleto().isBlank()) ? null : e.codigoBoleto();

        Context context = new Context();
        context.setVariable("motivo", motivo);
        context.setVariable("referencia", referencia);

        String html = templateEngine.process("correo-compra-fallida", context);

        enviar(e.emailUsuario(), "No pudimos completar tu compra - TicketFilms", html, null);
    }

    public void enviarReservaExpirada(ReservaExpiradaEvent e) {
        Context context = new Context();
        context.setVariable("asientos", String.join(", ", e.asientos()));
        context.setVariable("funcionId", e.funcionId());

        String html = templateEngine.process("correo-reserva-expirada", context);

        enviar(e.emailUsuario(), "Tu reserva expiró - TicketFilms", html, null);
    }

    private void enviar(String para, String asunto, String html, byte[] qr) {
        try {
            MimeMessage mensaje = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mensaje, true, "UTF-8");
            helper.setFrom(remitente);
            helper.setTo(para);
            helper.setSubject(asunto);
            helper.setText(html, true);
            if (qr != null) {
                helper.addInline("qr", new ByteArrayResource(qr), "image/png");
            }
            mailSender.send(mensaje);
        } catch (MessagingException ex) {
            throw new MailPreparationException("No se pudo armar el correo", ex);
        }
    }
}
