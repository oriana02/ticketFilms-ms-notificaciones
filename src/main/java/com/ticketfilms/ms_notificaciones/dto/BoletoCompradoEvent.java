package com.ticketfilms.ms_notificaciones.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

// Mismo formato que publica ms-boletos. Cada servicio tiene su propia copia:
// lo único que los une es el formato del mensaje.
public record BoletoCompradoEvent(
        String codigoBoleto,
        String emailUsuario,
        String tituloEvento,
        LocalDateTime fechaHoraFuncion,
        BigDecimal precioTotal,
        List<String> asientos
) {}
