package com.ticketfilms.ms_notificaciones.controller;

import com.ticketfilms.ms_notificaciones.dto.GrupoConsumidorDto;
import com.ticketfilms.ms_notificaciones.dto.TopicoDto;
import com.ticketfilms.ms_notificaciones.service.KafkaAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// Solo recibe, valida con @Valid y delega. Los errores los traduce
// GlobalExceptionHandler. Requiere el grupo admin de Cognito (SecurityConfig).
@RestController
@RequestMapping("/api/admin/kafka")
@RequiredArgsConstructor
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    @GetMapping("/topics")
    public ResponseEntity<Set<String>> listar() {
        return ResponseEntity.ok(kafkaAdminService.listarTopicos());
    }

    @GetMapping("/topics/{nombre}")
    public ResponseEntity<TopicoDto> obtener(@PathVariable String nombre) {
        return ResponseEntity.ok(kafkaAdminService.obtenerTopico(nombre));
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, String>> crear(@Valid @RequestBody CrearTopicoRequest req) {
        kafkaAdminService.crearTopico(req.nombre(), req.particiones(), req.replicas());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "mensaje", "Tópico creado",
                "nombre", req.nombre()));
    }

    @DeleteMapping("/topics/{nombre}")
    public ResponseEntity<Map<String, String>> eliminar(@PathVariable String nombre) {
        kafkaAdminService.eliminarTopico(nombre);
        return ResponseEntity.ok(Map.of(
                "mensaje", "Tópico eliminado",
                "nombre", nombre));
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<GrupoConsumidorDto>> listarGrupos() {
        return ResponseEntity.ok(kafkaAdminService.listarGrupos());
    }

    public record CrearTopicoRequest(
            @NotBlank String nombre,
            @Min(1) @Max(100) int particiones,
            @Min(1) @Max(10) int replicas) {

    }
}
