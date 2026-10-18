package com.ticketfilms.ms_notificaciones.exception;

public class TopicoYaExisteException extends RuntimeException {

    public TopicoYaExisteException(String nombre) {
        super("El tópico '" + nombre + "' ya existe");
    }
}
