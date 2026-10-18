package com.ticketfilms.ms_notificaciones.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.ticketfilms.ms_notificaciones.exception.TopicoNoEncontradoException;
import com.ticketfilms.ms_notificaciones.exception.TopicoYaExisteException;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaAdmin;

// Las validaciones se hacen ANTES de hablar con Kafka, por eso estos tests
// no necesitan un broker. Las consultas de existencia se simulan con un spy.
@ExtendWith(MockitoExtension.class)
class KafkaAdminServiceTest {

    @Mock
    private KafkaAdmin kafkaAdmin;

    @InjectMocks
    private KafkaAdminService service;

    // ============================================================
    // Validación de nombre (casos inválidos)
    // ============================================================
    @ParameterizedTest
    @ValueSource(strings = {"", " ", ".", "..", "con espacios", "con/barra", "tilde-ñ"})
    void crearTopico_nombreInvalido_lanzaIllegalArgument(String nombre) {
        assertThrows(IllegalArgumentException.class,
                () -> service.crearTopico(nombre, 1, 1));
    }

    @Test
    void crearTopico_nombreNulo_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> service.crearTopico(null, 1, 1));
    }

    @Test
    void crearTopico_nombreDemasiadoLargo_lanzaIllegalArgument() {
        String largo = "a".repeat(250);
        assertThrows(IllegalArgumentException.class,
                () -> service.crearTopico(largo, 1, 1));
    }

    @Test
    void obtenerTopico_nombreInvalido_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> service.obtenerTopico(".."));
    }

    @Test
    void eliminarTopico_nombreInvalido_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> service.eliminarTopico("con espacios"));
    }

    // ============================================================
    // Validación de configuración (particiones y réplicas)
    // ============================================================
    @ParameterizedTest
    @ValueSource(ints = {0, -1, 101})
    void crearTopico_particionesFueraDeRango_lanzaIllegalArgument(int particiones) {
        assertThrows(IllegalArgumentException.class,
                () -> service.crearTopico("topico.valido", particiones, 1));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, 11})
    void crearTopico_replicasFueraDeRango_lanzaIllegalArgument(int replicas) {
        assertThrows(IllegalArgumentException.class,
                () -> service.crearTopico("topico.valido", 1, replicas));
    }

    // ============================================================
    // Tópicos protegidos
    // ============================================================
    @ParameterizedTest
    @ValueSource(strings = {
        "boleto.comprado", "compra.fallida", "reserva.expirada",
        "boleto.comprado.DLT", "compra.fallida.DLT", "reserva.expirada.DLT"})
    void eliminarTopico_protegido_lanzaIllegalArgument(String nombre) {
        assertThrows(IllegalArgumentException.class,
                () -> service.eliminarTopico(nombre));
    }

    // ============================================================
    // Existencia (casos válidos que llegan hasta la consulta a Kafka)
    // ============================================================
    @Test
    void crearTopico_yaExiste_lanzaTopicoYaExiste() {
        KafkaAdminService conTopicos = spy(service);
        doReturn(Set.of("test.topic")).when(conTopicos).listarTopicos();

        assertThrows(TopicoYaExisteException.class,
                () -> conTopicos.crearTopico("test.topic", 1, 1));
    }

    @Test
    void eliminarTopico_noExiste_lanzaTopicoNoEncontrado() {
        KafkaAdminService conTopicos = spy(service);
        doReturn(Set.of("otro.topic")).when(conTopicos).listarTopicos();

        assertThrows(TopicoNoEncontradoException.class,
                () -> conTopicos.eliminarTopico("test.topic"));
    }

    @Test
    void obtenerTopico_noExiste_lanzaTopicoNoEncontrado() {
        KafkaAdminService conTopicos = spy(service);
        doReturn(Set.of("otro.topic")).when(conTopicos).listarTopicos();

        assertThrows(TopicoNoEncontradoException.class,
                () -> conTopicos.obtenerTopico("test.topic"));
    }
}
