package com.ticketfilms.ms_notificaciones.exception;

public class MensajeInvalidoException extends RuntimeException {

    public MensajeInvalidoException(String message) {
        super(message);
    }

    public MensajeInvalidoException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }

}
