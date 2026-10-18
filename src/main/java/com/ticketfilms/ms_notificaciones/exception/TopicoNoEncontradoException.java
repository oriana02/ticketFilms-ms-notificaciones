package com.ticketfilms.ms_notificaciones.exception;

public class TopicoNoEncontradoException extends RuntimeException {

    public TopicoNoEncontradoException(String nombre) {
        super("El tópico '" + nombre + "' no existe");
    }
}
