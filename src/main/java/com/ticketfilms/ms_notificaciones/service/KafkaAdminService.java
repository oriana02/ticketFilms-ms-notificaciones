package com.ticketfilms.ms_notificaciones.service;

import com.ticketfilms.ms_notificaciones.config.KafkaTopics;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public Set<String> listarTopicos() {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return client.listTopics().names().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Listado de tópicos interrumpido", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudieron listar los tópicos", e);
        }
    }

    public void crearTopico(String nombre, int particiones, int replicas) {
        validarNombre(nombre);
        validarParticiones(particiones);
        validarReplicas(replicas);

        try {
            NewTopic nuevo = new NewTopic(nombre, particiones, (short) replicas);
            kafkaAdmin.createOrModifyTopics(nuevo);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo crear el tópico " + nombre, e);
        }
    }

    public void eliminarTopico(String nombre) {
        validarNombre(nombre);
        if (TOPICOS_PROTEGIDOS.contains(nombre)) {
            throw new IllegalArgumentException("El tópico '" + nombre + "' está protegido y no se puede eliminar");
        }

        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            client.deleteTopics(List.of(nombre)).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Eliminación interrumpida", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo eliminar el tópico " + nombre, e);
        }
    }

    private void validarNombre(String nombre) {
        if (nombre == null || nombre.isBlank()) {
            throw new IllegalArgumentException("El nombre del tópico es obligatorio");
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
