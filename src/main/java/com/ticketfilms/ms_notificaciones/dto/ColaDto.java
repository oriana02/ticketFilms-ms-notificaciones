package com.ticketfilms.ms_notificaciones.dto;

// Detalle de una cola para la API admin. "protegida" = cola del sistema que no se puede eliminar.
public record ColaDto(String nombre, long mensajes, int consumidores, boolean protegida) {

}
