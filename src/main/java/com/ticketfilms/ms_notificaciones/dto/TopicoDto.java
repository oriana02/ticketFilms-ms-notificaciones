package com.ticketfilms.ms_notificaciones.dto;

// Detalle de un tópico para la API admin. "protegido" indica si es un tópico
// del sistema que no se puede eliminar.
public record TopicoDto(String nombre, int particiones, int replicas, boolean protegido) {}
