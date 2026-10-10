package com.ticketfilms.ms_notificaciones.dto;

import java.util.List;

// OJO: ajustar los campos a lo que realmente publique ms-asientos.
public record ReservaExpiradaEvent(Long funcionId, List<String> asientos, String emailUsuario) {}
