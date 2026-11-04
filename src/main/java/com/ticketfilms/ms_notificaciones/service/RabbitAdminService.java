package com.ticketfilms.ms_notificaciones.service;

import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.COLAS;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.EX_DLX;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.EX_EVENTOS;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.SUFIJO_DLQ;
import static com.ticketfilms.ms_notificaciones.config.RabbitTopology.dlq;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import com.ticketfilms.ms_notificaciones.dto.ColaDto;
import com.ticketfilms.ms_notificaciones.exception.RecursoNoEncontradoException;
import com.ticketfilms.ms_notificaciones.exception.RecursoYaExisteException;

import lombok.RequiredArgsConstructor;

// Toda la lógica de crear, consultar y eliminar colas, exchanges y bindings.
// Las validaciones se hacen ANTES de hablar con RabbitMQ.
@Service
@RequiredArgsConstructor
public class RabbitAdminService {

    private static final Pattern NOMBRE_VALIDO = Pattern.compile("^[a-zA-Z0-9._-]{1,255}$");
    private static final Set<String> TIPOS_EXCHANGE = Set.of("direct", "topic", "fanout");

    // Infraestructura del sistema: no se puede eliminar desde la API admin.
    private static final Set<String> COLAS_PROTEGIDAS = Stream.concat(
            COLAS.stream(), COLAS.stream().map(c -> dlq(c))).collect(Collectors.toUnmodifiableSet());
    private static final Set<String> EXCHANGES_PROTEGIDOS = Set.of(EX_EVENTOS, EX_DLX);

    private final AmqpAdmin amqpAdmin;
    private final RestClient rabbitManagementClient;

    // ============================================================
    // Consultas
    // ============================================================
    public Set<String> listarColas() {
        return nombres("/api/queues");
    }

    public Set<String> listarExchanges() {
        return nombres("/api/exchanges");
    }

    public ColaDto obtenerCola(String nombre) {
        validarNombre(nombre, "cola");
        QueueInformation info = amqpAdmin.getQueueInfo(nombre);
        if (info == null) {
            throw new RecursoNoEncontradoException("cola", nombre);
        }
        return new ColaDto(nombre, info.getMessageCount(), info.getConsumerCount(),
                COLAS_PROTEGIDAS.contains(nombre));
    }

    // ============================================================
    // Colas
    // ============================================================
    // Si conDlq es true, crea también "<nombre>.dlq" y deja la cola apuntando al DLX.
    // Si se indica exchange, enlaza la cola a ese exchange con la routing key.
    public void crearCola(String nombre, String exchange, String routingKey, boolean conDlq) {
        validarNombre(nombre, "cola");
        if (nombre.endsWith(SUFIJO_DLQ)) {
            throw new IllegalArgumentException("Los nombres que terminan en '" + SUFIJO_DLQ
                    + "' están reservados para las DLQ; se crean solas con conDlq=true");
        }
        boolean enlazar = exchange != null && !exchange.isBlank();
        if (enlazar) {
            validarNombre(exchange, "exchange");
            if (routingKey == null || routingKey.isBlank()) {
                throw new IllegalArgumentException("Si indicas un exchange, la routingKey es obligatoria");
            }
        }
        if (listarColas().contains(nombre)) {
            throw new RecursoYaExisteException("cola", nombre);
        }
        if (enlazar && !listarExchanges().contains(exchange)) {
            throw new RecursoNoEncontradoException("exchange", exchange);
        }

        QueueBuilder constructor = QueueBuilder.durable(nombre);
        if (conDlq) {
            constructor.deadLetterExchange(EX_DLX).deadLetterRoutingKey(dlq(nombre));
        }
        amqpAdmin.declareQueue(constructor.build());

        if (conDlq) {
            Queue colaDlq = QueueBuilder.durable(dlq(nombre)).build();
            amqpAdmin.declareQueue(colaDlq);
            amqpAdmin.declareBinding(binding(EX_DLX, dlq(nombre), dlq(nombre)));
        }
        if (enlazar) {
            amqpAdmin.declareBinding(binding(exchange, nombre, routingKey));
        }
    }

    public void eliminarCola(String nombre) {
        validarNombre(nombre, "cola");
        if (COLAS_PROTEGIDAS.contains(nombre)) {
            throw new IllegalArgumentException("La cola '" + nombre + "' está protegida y no se puede eliminar");
        }
        if (!listarColas().contains(nombre)) {
            throw new RecursoNoEncontradoException("cola", nombre);
        }
        amqpAdmin.deleteQueue(nombre);
    }

    // ============================================================
    // Exchanges
    // ============================================================
    public void crearExchange(String nombre, String tipo) {
        validarNombre(nombre, "exchange");
        if (tipo == null || !TIPOS_EXCHANGE.contains(tipo.toLowerCase())) {
            throw new IllegalArgumentException("Tipo de exchange inválido: use direct, topic o fanout");
        }
        if (listarExchanges().contains(nombre)) {
            throw new RecursoYaExisteException("exchange", nombre);
        }
        Exchange exchange = switch (tipo.toLowerCase()) {
            case "direct" -> new DirectExchange(nombre, true, false);
            case "topic" -> new TopicExchange(nombre, true, false);
            default -> new FanoutExchange(nombre, true, false);
        };
        amqpAdmin.declareExchange(exchange);
    }

    public void eliminarExchange(String nombre) {
        validarNombre(nombre, "exchange");
        if (EXCHANGES_PROTEGIDOS.contains(nombre)) {
            throw new IllegalArgumentException("El exchange '" + nombre + "' está protegido y no se puede eliminar");
        }
        if (!listarExchanges().contains(nombre)) {
            throw new RecursoNoEncontradoException("exchange", nombre);
        }
        amqpAdmin.deleteExchange(nombre);
    }

    // ============================================================
    // Bindings
    // ============================================================
    public void crearBinding(String exchange, String cola, String routingKey) {
        validarBinding(exchange, cola, routingKey);
        amqpAdmin.declareBinding(binding(exchange, cola, routingKey));
    }

    public void eliminarBinding(String exchange, String cola, String routingKey) {
        validarBinding(exchange, cola, routingKey);
        if (COLAS_PROTEGIDAS.contains(cola)) {
            throw new IllegalArgumentException("Los bindings de la cola protegida '" + cola + "' no se pueden eliminar");
        }
        amqpAdmin.removeBinding(binding(exchange, cola, routingKey));
    }

    // ============================================================
    // Internos
    // ============================================================
    private Binding binding(String exchange, String cola, String routingKey) {
        return new Binding(cola, DestinationType.QUEUE, exchange, routingKey, Map.of());
    }

    private void validarBinding(String exchange, String cola, String routingKey) {
        validarNombre(exchange, "exchange");
        validarNombre(cola, "cola");
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("La routingKey es obligatoria");
        }
        if (!listarExchanges().contains(exchange)) {
            throw new RecursoNoEncontradoException("exchange", exchange);
        }
        if (!listarColas().contains(cola)) {
            throw new RecursoNoEncontradoException("cola", cola);
        }
    }

    private void validarNombre(String valor, String que) {
        if (valor == null || valor.isBlank()) {
            throw new IllegalArgumentException("El nombre de " + que + " es obligatorio");
        }
        if (!NOMBRE_VALIDO.matcher(valor).matches()) {
            throw new IllegalArgumentException("Nombre de " + que
                    + " inválido: solo letras, números, punto, guion y guion bajo (1..255 caracteres)");
        }
        if (valor.startsWith("amq.")) {
            throw new IllegalArgumentException("Los nombres que empiezan con 'amq.' están reservados por RabbitMQ");
        }
    }

    // Lista nombres desde la API de gestión (AMQP no permite listar). Solo el vhost "/".
    private Set<String> nombres(String ruta) {
        try {
            List<Map<String, Object>> items = rabbitManagementClient.get().uri(ruta).retrieve()
                    .body(new ParameterizedTypeReference<List<Map<String, Object>>>() {
                    });
            if (items == null) {
                return Set.of();
            }
            return items.stream()
                    .filter(i -> "/".equals(i.get("vhost")))
                    .map(i -> String.valueOf(i.get("name")))
                    .filter(n -> !n.isBlank() && !n.startsWith("amq."))
                    .collect(Collectors.toCollection(TreeSet::new));
        } catch (RestClientException e) {
            throw new IllegalStateException("No se pudo consultar la API de gestión de RabbitMQ", e);
        }
    }
}
