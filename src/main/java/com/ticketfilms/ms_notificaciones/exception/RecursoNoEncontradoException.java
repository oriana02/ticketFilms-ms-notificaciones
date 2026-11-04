package com.ticketfilms.ms_notificaciones.exception;

public class RecursoNoEncontradoException extends RuntimeException {

    public RecursoNoEncontradoException(String tipo, String nombre) {
        super("El " + tipo + " '" + nombre + "' no existe");
    }
}
