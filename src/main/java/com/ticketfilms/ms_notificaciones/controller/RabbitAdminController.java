package com.ticketfilms.ms_notificaciones.controller;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ticketfilms.ms_notificaciones.dto.ColaDto;
import com.ticketfilms.ms_notificaciones.dto.FalloRegistrado;
import com.ticketfilms.ms_notificaciones.listener.DlqRegistro;
import com.ticketfilms.ms_notificaciones.service.RabbitAdminService;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;

// Solo recibe, valida con @Valid y delega. Los errores los traduce GlobalExceptionHandler.
// Requiere el grupo admin de Cognito (SecurityConfig).
@RestController
@RequestMapping("/api/admin/rabbit")
@RequiredArgsConstructor
public class RabbitAdminController {

    private static final String NOMBRE = "^[a-zA-Z0-9._-]{1,255}$";
    private static final String MSG_NOMBRE = "solo letras, números, punto, guion y guion bajo (1..255 caracteres)";

    private final RabbitAdminService rabbitAdminService;
    private final DlqRegistro dlqRegistro;

    // ---------- Colas ----------
    @GetMapping("/queues")
    public ResponseEntity<Set<String>> listarColas() {
        return ResponseEntity.ok(rabbitAdminService.listarColas());
    }

    @GetMapping("/queues/{nombre}")
    public ResponseEntity<ColaDto> obtenerCola(@PathVariable String nombre) {
        return ResponseEntity.ok(rabbitAdminService.obtenerCola(nombre));
    }

    @PostMapping("/queues")
    public ResponseEntity<Map<String, String>> crearCola(@Valid @RequestBody CrearColaRequest req) {
        boolean conDlq = req.conDlq() == null || req.conDlq();
        rabbitAdminService.crearCola(req.nombre(), req.exchange(), req.routingKey(), conDlq);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "mensaje", "Cola creada", "nombre", req.nombre()));
    }

    @DeleteMapping("/queues/{nombre}")
    public ResponseEntity<Map<String, String>> eliminarCola(@PathVariable String nombre) {
        rabbitAdminService.eliminarCola(nombre);
        return ResponseEntity.ok(Map.of("mensaje", "Cola eliminada", "nombre", nombre));
    }

    // ---------- Exchanges ----------
    @GetMapping("/exchanges")
    public ResponseEntity<Set<String>> listarExchanges() {
        return ResponseEntity.ok(rabbitAdminService.listarExchanges());
    }

    @PostMapping("/exchanges")
    public ResponseEntity<Map<String, String>> crearExchange(@Valid @RequestBody CrearExchangeRequest req) {
        rabbitAdminService.crearExchange(req.nombre(), req.tipo());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "mensaje", "Exchange creado", "nombre", req.nombre(), "tipo", req.tipo()));
    }

    @DeleteMapping("/exchanges/{nombre}")
    public ResponseEntity<Map<String, String>> eliminarExchange(@PathVariable String nombre) {
        rabbitAdminService.eliminarExchange(nombre);
        return ResponseEntity.ok(Map.of("mensaje", "Exchange eliminado", "nombre", nombre));
    }

    // ---------- Bindings ----------
    @PostMapping("/bindings")
    public ResponseEntity<Map<String, String>> crearBinding(@Valid @RequestBody BindingRequest req) {
        rabbitAdminService.crearBinding(req.exchange(), req.cola(), req.routingKey());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("mensaje", "Binding creado"));
    }

    @DeleteMapping("/bindings")
    public ResponseEntity<Map<String, String>> eliminarBinding(@Valid @RequestBody BindingRequest req) {
        rabbitAdminService.eliminarBinding(req.exchange(), req.cola(), req.routingKey());
        return ResponseEntity.ok(Map.of("mensaje", "Binding eliminado"));
    }

    // ---------- DLQ: últimos mensajes fallidos con su contexto ----------
    @GetMapping("/dlq/mensajes")
    public ResponseEntity<List<FalloRegistrado>> mensajesFallidos() {
        return ResponseEntity.ok(dlqRegistro.ultimos());
    }

    // ---------- Requests ----------
    public record CrearColaRequest(
            @NotBlank(message = "el nombre es obligatorio")
            @Pattern(regexp = NOMBRE, message = MSG_NOMBRE) String nombre,
            @Pattern(regexp = NOMBRE, message = MSG_NOMBRE) String exchange,
            String routingKey,
            Boolean conDlq) {

    }

    public record CrearExchangeRequest(
            @NotBlank(message = "el nombre es obligatorio")
            @Pattern(regexp = NOMBRE, message = MSG_NOMBRE) String nombre,
            @NotBlank(message = "el tipo es obligatorio")
            @Pattern(regexp = "^(direct|topic|fanout)$", message = "debe ser direct, topic o fanout") String tipo) {

    }

    public record BindingRequest(
            @NotBlank(message = "el exchange es obligatorio")
            @Pattern(regexp = NOMBRE, message = MSG_NOMBRE) String exchange,
            @NotBlank(message = "la cola es obligatoria")
            @Pattern(regexp = NOMBRE, message = MSG_NOMBRE) String cola,
            @NotBlank(message = "la routingKey es obligatoria") String routingKey) {

    }
}
