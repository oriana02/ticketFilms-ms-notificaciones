package com.ticketfilms.ms_notificaciones.controller;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.ticketfilms.ms_notificaciones.service.KafkaAdminService;

@RestController
@RequestMapping("/api/admin/kafka/topics")
@RequiredArgsConstructor
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    @GetMapping
    public ResponseEntity<Set<String>> listar() {
        return ResponseEntity.ok(kafkaAdminService.listarTopicos());
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> crear(@RequestBody CrearTopicoRequest req) {
        kafkaAdminService.crearTopico(req.nombre(), req.particiones(), req.replicas());
        return ResponseEntity.ok(Map.of(
                "mensaje", "Tópico creado",
                "nombre", req.nombre()));
    }

    @DeleteMapping("/{nombre}")
    public ResponseEntity<Map<String, String>> eliminar(@PathVariable String nombre) {
        kafkaAdminService.eliminarTopico(nombre);
        return ResponseEntity.ok(Map.of(
                "mensaje", "Tópico eliminado",
                "nombre", nombre));
    }

    // --- DTO y manejo de errores locales ---
    public record CrearTopicoRequest(
            @NotBlank String nombre,
            @Min(1) int particiones,
            @Min(1) int replicas) {

    }

    @org.springframework.web.bind.annotation.ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> serverError(IllegalStateException e) {
        return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
    }
}
