package com.ticketfilms.ms_notificaciones.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.web.client.RestClient;

import com.ticketfilms.ms_notificaciones.exception.RecursoNoEncontradoException;
import com.ticketfilms.ms_notificaciones.exception.RecursoYaExisteException;

// Las validaciones ocurren ANTES de hablar con RabbitMQ, por eso no hace falta un broker.
// Las consultas de existencia se simulan con un spy.
@ExtendWith(MockitoExtension.class)
class RabbitAdminServiceTest {

    @Mock
    private AmqpAdmin amqpAdmin;

    @Mock
    private RestClient rabbitManagementClient;

    @InjectMocks
    private RabbitAdminService service;

    // ---------- Validación de nombres ----------
    @ParameterizedTest
    @ValueSource(strings = {"", " ", "con espacios", "con/barra", "tilde-ñ", "amq.reservado"})
    void crearCola_nombreInvalido_lanzaIllegalArgument(String nombre) {
        assertThrows(IllegalArgumentException.class, () -> service.crearCola(nombre, null, null, true));
    }

    @Test
    void crearCola_nombreNulo_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> service.crearCola(null, null, null, true));
    }

    @Test
    void crearCola_nombreDemasiadoLargo_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> service.crearCola("a".repeat(300), null, null, true));
    }

    @Test
    void crearCola_nombreTerminaEnDlq_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> service.crearCola("q.mia.dlq", null, null, true));
    }

    @Test
    void crearCola_exchangeSinRoutingKey_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> service.crearCola("q.nueva", "ex.algo", " ", true));
    }

    // ---------- Exchanges ----------
    @ParameterizedTest
    @ValueSource(strings = {"headers", "", "otro"})
    void crearExchange_tipoInvalido_lanzaIllegalArgument(String tipo) {
        assertThrows(IllegalArgumentException.class, () -> service.crearExchange("ex.nuevo", tipo));
    }

    @Test
    void crearExchange_tipoNulo_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> service.crearExchange("ex.nuevo", null));
    }

    // ---------- Bindings ----------
    @Test
    void crearBinding_sinRoutingKey_lanzaIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> service.crearBinding("ex.a", "q.b", ""));
    }

    // ---------- Protegidos ----------
    @ParameterizedTest
    @ValueSource(strings = {
        "q.boleto.comprado.notificaciones", "q.compra.fallida.notificaciones",
        "q.compra.fallida.asientos", "q.reserva.expirada.notificaciones",
        "q.boleto.comprado.notificaciones.dlq", "q.reserva.expirada.notificaciones.dlq"})
    void eliminarCola_protegida_lanzaIllegalArgument(String nombre) {
        assertThrows(IllegalArgumentException.class, () -> service.eliminarCola(nombre));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ex.tickets.eventos", "ex.tickets.dlx"})
    void eliminarExchange_protegido_lanzaIllegalArgument(String nombre) {
        assertThrows(IllegalArgumentException.class, () -> service.eliminarExchange(nombre));
    }

    // ---------- Existencia ----------
    @Test
    void crearCola_yaExiste_lanzaRecursoYaExiste() {
        RabbitAdminService conColas = spy(service);
        doReturn(Set.of("q.test")).when(conColas).listarColas();

        assertThrows(RecursoYaExisteException.class, () -> conColas.crearCola("q.test", null, null, true));
    }

    @Test
    void eliminarCola_noExiste_lanzaRecursoNoEncontrado() {
        RabbitAdminService conColas = spy(service);
        doReturn(Set.of("q.otra")).when(conColas).listarColas();

        assertThrows(RecursoNoEncontradoException.class, () -> conColas.eliminarCola("q.test"));
    }

    @Test
    void crearExchange_yaExiste_lanzaRecursoYaExiste() {
        RabbitAdminService conExchanges = spy(service);
        doReturn(Set.of("ex.test")).when(conExchanges).listarExchanges();

        assertThrows(RecursoYaExisteException.class, () -> conExchanges.crearExchange("ex.test", "topic"));
    }
}
