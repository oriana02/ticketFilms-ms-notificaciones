package com.ticketfilms.ms_notificaciones.exception;

public class RecursoYaExisteException extends RuntimeException {

    public RecursoYaExisteException(String tipo, String nombre) {
        super("El " + tipo + " '" + nombre + "' ya existe");
    }
}
