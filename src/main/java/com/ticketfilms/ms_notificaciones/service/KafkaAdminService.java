package com.ticketfilms.ms_notificaciones.service;

import com.ticketfilms.ms_notificaciones.config.KafkaTopics;
import com.ticketfilms.ms_notificaciones.dto.GrupoConsumidorDto;
import com.ticketfilms.ms_notificaciones.dto.TopicoDto;
import com.ticketfilms.ms_notificaciones.exception.TopicoNoEncontradoException;
import com.ticketfilms.ms_notificaciones.exception.TopicoYaExisteException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaAdminService {

    // Solo letras, números, punto, guion y guion bajo. Longitud 1..249.
    private static final Pattern NOMBRE_VALIDO = Pattern.compile("^[a-zA-Z0-9._-]{1,249}$");

    // Tópicos del sistema que NO se pueden borrar desde la API admin.
    private static final Set<String> TOPICOS_PROTEGIDOS = Set.of(
            KafkaTopics.BOLETO_COMPRADO,
            KafkaTopics.COMPRA_FALLIDA,
            KafkaTopics.RESERVA_EXPIRADA,
            KafkaTopics.BOLETO_COMPRADO_DLT,
            KafkaTopics.COMPRA_FALLIDA_DLT,
            KafkaTopics.RESERVA_EXPIRADA_DLT
    );

    private final KafkaAdmin kafkaAdmin;

    @FunctionalInterface
    private interface Operacion<T> {

        T ejecutar(AdminClient client) throws ExecutionException, InterruptedException;
    }

    public Set<String> listarTopicos() {
        return conAdmin(client -> client.listTopics().names().get(),
                "No se pudieron listar los tópicos");
    }

    public TopicoDto obtenerTopico(String nombre) {
        validarNombre(nombre);
        exigirQueExista(nombre);

        TopicDescription descripcion = conAdmin(
                client -> client.describeTopics(List.of(nombre)).allTopicNames().get().get(nombre),
                "No se pudo consultar el tópico " + nombre);

        int replicas = descripcion.partitions().isEmpty()
                ? 0
                : descripcion.partitions().get(0).replicas().size();
        return new TopicoDto(nombre, descripcion.partitions().size(), replicas,
                TOPICOS_PROTEGIDOS.contains(nombre));
    }

    public void crearTopico(String nombre, int particiones, int replicas) {
        validarNombre(nombre);
        validarParticiones(particiones);
        validarReplicas(replicas);

        if (listarTopicos().contains(nombre)) {
            throw new TopicoYaExisteException(nombre);
        }

        conAdmin(client -> {
            client.createTopics(List.of(new NewTopic(nombre, particiones, (short) replicas)))
                    .all().get();
            return null;
        }, "No se pudo crear el tópico " + nombre);
    }

    public void eliminarTopico(String nombre) {
        validarNombre(nombre);
        if (TOPICOS_PROTEGIDOS.contains(nombre)) {
            throw new IllegalArgumentException("El tópico '" + nombre + "' está protegido y no se puede eliminar");
        }
        exigirQueExista(nombre);

        conAdmin(client -> {
            client.deleteTopics(List.of(nombre)).all().get();
            return null;
        }, "No se pudo eliminar el tópico " + nombre);
    }

    // Grupos de consumidores y los tópicos que escuchan ahora mismo.
    // Un grupo sin consumidores conectados aparece con la lista de tópicos vacía.
    public List<GrupoConsumidorDto> listarGrupos() {
        return conAdmin(client -> {
            List<String> ids = client.listConsumerGroups().all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .toList();
            if (ids.isEmpty()) {
                return List.<GrupoConsumidorDto>of();
            }
            Map<String, ConsumerGroupDescription> descripciones
                    = client.describeConsumerGroups(ids).all().get();
            return descripciones.values().stream()
                    .map(d -> new GrupoConsumidorDto(
                    d.groupId(),
                    String.valueOf(d.groupState()),
                    d.members().stream()
                            .flatMap(m -> m.assignment().topicPartitions().stream())
                            .map(TopicPartition::topic)
                            .collect(java.util.stream.Collectors.toCollection(TreeSet::new))))
                    .sorted(Comparator.comparing(GrupoConsumidorDto::grupoId))
                    .toList();
        }, "No se pudieron listar los grupos de consumidores");
    }

    // Abre y cierra el AdminClient en un solo lugar para todas las operaciones.
    private <T> T conAdmin(Operacion<T> operacion, String mensajeError) {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return operacion.ejecutar(client);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(mensajeError + " (operación interrumpida)", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(mensajeError, e);
        }
    }

    private void exigirQueExista(String nombre) {
        if (!listarTopicos().contains(nombre)) {
            throw new TopicoNoEncontradoException(nombre);
        }
    }

    private void validarNombre(String nombre) {
        if (nombre == null || nombre.isBlank()) {
            throw new IllegalArgumentException("El nombre del tópico es obligatorio");
        }
        if (".".equals(nombre) || "..".equals(nombre)) {
            throw new IllegalArgumentException("Nombre inválido: '.' y '..' no están permitidos por Kafka");
        }
        if (!NOMBRE_VALIDO.matcher(nombre).matches()) {
            throw new IllegalArgumentException(
                    "Nombre inválido: solo letras, números, punto, guion y guion bajo (1..249 caracteres)");
        }
    }

    private void validarParticiones(int particiones) {
        if (particiones < 1 || particiones > 100) {
            throw new IllegalArgumentException("Las particiones deben estar entre 1 y 100");
        }
    }

    private void validarReplicas(int replicas) {
        if (replicas < 1 || replicas > 10) {
            throw new IllegalArgumentException("Las réplicas deben estar entre 1 y 10");
        }
    }
}
