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
import org.springframework.web.util.HtmlUtils;

// Arma y envía los correos. Si falla, la excepción sube al listener y entra
// el flujo de reintentos y DLT configurado en KafkaErrorHandlingConfig.
@Service
public class EmailService {

    private static final DateTimeFormatter FORMATO_FECHA
            = DateTimeFormatter.ofPattern("dd-MM-yyyy 'a las' HH:mm");

    private final JavaMailSender mailSender;
    private final QrService qrService;

    @Value("${app.mail.from}")
    private String remitente;

    public EmailService(JavaMailSender mailSender, QrService qrService) {
        this.mailSender = mailSender;
        this.qrService = qrService;
    }

    public void enviarConfirmacion(BoletoCompradoEvent e) {
        byte[] qr = qrService.generarPng(e.codigoBoleto());

        String fecha = e.fechaHoraFuncion().format(FORMATO_FECHA);
        String total = NumberFormat.getIntegerInstance(Locale.forLanguageTag("es-CL"))
                .format(e.precioTotal());

        String html = """
                <h2>¡Compra confirmada!</h2>
                <p><b>%s</b></p>
                <p>Función: %s</p>
                <p>Asientos: %s</p>
                <p>Total: $%s</p>
                <p>Código de tu boleto: <b>%s</b></p>
                <p>Presenta este QR en la entrada:</p>
                <img src="cid:qr" alt="QR del boleto" width="200" height="200"/>
                """.formatted(
                HtmlUtils.htmlEscape(e.tituloEvento()),
                fecha,
                HtmlUtils.htmlEscape(String.join(", ", e.asientos())),
                total,
                HtmlUtils.htmlEscape(e.codigoBoleto()));

        enviar(e.emailUsuario(), "Tu boleto " + e.codigoBoleto() + " - TicketFilms", html, qr);
    }

    public void enviarCompraFallida(CompraFallidaEvent e) {
        String motivo = (e.motivo() == null || e.motivo().isBlank())
                ? "No se pudo completar el pago"
                : e.motivo();

        String referencia = (e.codigoBoleto() == null || e.codigoBoleto().isBlank())
                ? ""
                : "<p style=\"color:#666;font-size:12px\">Referencia: %s</p>"
                        .formatted(HtmlUtils.htmlEscape(e.codigoBoleto()));

        String html = """
                <h2>No pudimos completar tu compra</h2>
                <p>Motivo: %s</p>
                <p>Los asientos que reservaste fueron liberados. Puedes volver a intentarlo desde TicketFilms.</p>
                %s
                """.formatted(HtmlUtils.htmlEscape(motivo), referencia);

        enviar(e.emailUsuario(), "No pudimos completar tu compra - TicketFilms", html, null);
    }

    public void enviarReservaExpirada(ReservaExpiradaEvent e) {
        String html = """
            <h2>Tu reserva expiró</h2>
            <p>Los asientos %s de la función %s volvieron a estar disponibles
            porque pasaron los 5 minutos de reserva.</p>
            <p>Si aún quieres ir, vuelve a seleccionarlos desde TicketFilms.</p>
            """.formatted(HtmlUtils.htmlEscape(String.join(", ", e.asientos())), e.funcionId());
        enviar(e.emailUsuario(), "Tu reserva expiró - TicketFilms", html, null);
    }

    private void enviar(String para, String asunto, String html, byte[] qr) {
        try {
            MimeMessage mensaje = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mensaje, true, "UTF-8");
            helper.setFrom(remitente);
            helper.setTo(para);
            helper.setSubject(asunto);
            helper.setText(html, true);                                          // primero el texto
            if (qr != null) {
                helper.addInline("qr", new ByteArrayResource(qr), "image/png");  // luego la imagen
            }
            mailSender.send(mensaje);
        } catch (MessagingException ex) {
            throw new MailPreparationException("No se pudo armar el correo", ex);
        }
    }
}
