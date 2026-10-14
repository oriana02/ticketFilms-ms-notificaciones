package com.ticketfilms.ms_notificaciones.config;

import com.ticketfilms.ms_notificaciones.exception.MensajeInvalidoException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

// Qué pasa cuando un @KafkaListener falla al procesar un mensaje:
//
//  1) Error RECUPERABLE (ej. un servicio externo no responde):
//     se reintenta hasta 3 veces, esperando 1s, 2s y 4s entre intentos.
//  2) Error DEFINITIVO (MensajeInvalidoException: JSON malformado o
//     incompleto): no se reintenta, va directo al tópico de fallidos.
//  3) Si se agotan los reintentos: el mensaje se manda al tópico
//     "<tópico original>.DLT" y se deja en el log tópico, partición,
//     offset y motivo.
//  4) Solo cuando el mensaje se procesó bien (o ya quedó en el DLT)
//     se confirma el offset, así no se pierde ni se reprocesa sin control.
//
// Los beans de infraestructura (ProducerFactory, KafkaTemplate, KafkaAdmin)
// se declaran explícitamente para no depender de la autoconfiguración de
// Spring Boot 4.x, que ya no la activa solo con spring-kafka en el classpath.
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    private static final int MAX_REINTENTOS = 3;
    private static final long ESPERA_INICIAL_MS = 1000L;
    private static final double MULTIPLICADOR = 2.0;
    private static final long ESPERA_MAXIMA_MS = 10000L;

    // ============================================================
    // Infraestructura Kafka (producer, template, admin)
    // ============================================================
    // Fábrica de productores: se usa para publicar en los tópicos DLT.
    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // KafkaTemplate: lo usa el DeadLetterPublishingRecoverer para publicar en el DLT.
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // KafkaAdmin: lo usa KafkaAdminService para crear/listar/eliminar tópicos.
    // También procesa los @Bean NewTopic para crearlos al arrancar.
    @Bean
    public KafkaAdmin kafkaAdmin(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        KafkaAdmin admin = new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
        // No fallar si el broker no está disponible al arranque.
        admin.setFatalIfBrokerNotAvailable(false);
        return admin;
    }

    // ============================================================
    // Manejo de errores
    // ============================================================
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {

        // Manda el mensaje fallido a "<topico>.DLT". Partición -1 = que Kafka elija.
        DeadLetterPublishingRecoverer publicador = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (registro, ex) -> new TopicPartition(registro.topic() + KafkaTopics.SUFIJO_DLT, -1));

        // Publica en el DLT y luego deja registro en el log para diagnóstico.
        ConsumerRecordRecoverer conRegistro = (registro, ex) -> {
            publicador.accept(registro, ex);
            log.error("Mensaje enviado a DLT — topico={} particion={} offset={} motivo={} mensaje={}",
                    registro.topic(), registro.partition(), registro.offset(),
                    ex.getMessage(), registro.value(), ex);
        };

        ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(MAX_REINTENTOS);
        espera.setInitialInterval(ESPERA_INICIAL_MS);
        espera.setMultiplier(MULTIPLICADOR);
        espera.setMaxInterval(ESPERA_MAXIMA_MS);

        DefaultErrorHandler handler = new DefaultErrorHandler(conRegistro, espera);

        // Errores definitivos: sin reintentos, directo al DLT
        handler.addNotRetryableExceptions(MensajeInvalidoException.class);

        // Un log por cada reintento, para ver el proceso en la demo
        handler.setRetryListeners((registro, ex, intento)
                -> log.warn("Reintento {} de {} — topico={} offset={} motivo={}",
                        intento, MAX_REINTENTOS, registro.topic(), registro.offset(), ex.getMessage()));

        return handler;
    }

    // ============================================================
    // Tópicos de negocio (los que escuchan los listeners)
    // ============================================================
    @Bean
    public NewTopic boletoComprado() {
        return TopicBuilder.name(KafkaTopics.BOLETO_COMPRADO)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic compraFallida() {
        return TopicBuilder.name(KafkaTopics.COMPRA_FALLIDA)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic reservaExpirada() {
        return TopicBuilder.name(KafkaTopics.RESERVA_EXPIRADA)
                .partitions(1).replicas(1).build();
    }

    // ============================================================
    // Tópicos de fallidos (DLT)
    // ============================================================
    @Bean
    public NewTopic boletoCompradoDlt() {
        return TopicBuilder.name(KafkaTopics.BOLETO_COMPRADO_DLT)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic compraFallidaDlt() {
        return TopicBuilder.name(KafkaTopics.COMPRA_FALLIDA_DLT)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic reservaExpiradaDlt() {
        return TopicBuilder.name(KafkaTopics.RESERVA_EXPIRADA_DLT)
                .partitions(1).replicas(1).build();
    }
}
