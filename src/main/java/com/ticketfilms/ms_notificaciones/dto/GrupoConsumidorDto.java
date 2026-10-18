package com.ticketfilms.ms_notificaciones.dto;

import java.util.Set;

// Un grupo de consumidores y los tópicos que tiene asignados en este momento.
public record GrupoConsumidorDto(String grupoId, String estado, Set<String> topicos) {}
