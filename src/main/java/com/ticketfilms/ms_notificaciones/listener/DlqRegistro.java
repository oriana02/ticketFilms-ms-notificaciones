package com.ticketfilms.ms_notificaciones.listener;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.stereotype.Component;

import com.ticketfilms.ms_notificaciones.dto.FalloRegistrado;

// Últimos mensajes fallidos en memoria (máx. 100) para consultarlos desde la API admin.
@Component
public class DlqRegistro {

    private static final int MAXIMO = 100;
    private final Deque<FalloRegistrado> fallos = new ConcurrentLinkedDeque<>();

    public void agregar(FalloRegistrado fallo) {
        fallos.addFirst(fallo);
        while (fallos.size() > MAXIMO) {
            fallos.pollLast();
        }
    }

    public List<FalloRegistrado> ultimos() {
        return List.copyOf(fallos);
    }
}
