package com.ticketfilms.ms_notificaciones.config;

// Único lugar donde se escriben los nombres de los tópicos.
// Listeners, config de errores y admin usan estas constantes,
// así un cambio de nombre se hace una sola vez.
public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String BOLETO_COMPRADO = "boleto.comprado";
    public static final String COMPRA_FALLIDA = "compra.fallida";
    public static final String RESERVA_EXPIRADA = "reserva.expirada";

    // Sufijo y tópicos de mensajes fallidos (DLT)
    public static final String SUFIJO_DLT = ".DLT";

    public static final String BOLETO_COMPRADO_DLT = BOLETO_COMPRADO + SUFIJO_DLT;
    public static final String COMPRA_FALLIDA_DLT = COMPRA_FALLIDA + SUFIJO_DLT;
    public static final String RESERVA_EXPIRADA_DLT = RESERVA_EXPIRADA + SUFIJO_DLT;
}
