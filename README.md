# ms-notificaciones

Microservicio de **TicketFilms** que avisa a los usuarios cuando pasa algo con sus compras o reservas. No tiene base de datos: **escucha mensajes de Apache Kafka** y reacciona a ellos, y expone una **API de administración** para gestionar tópicos.

> Estado actual: los avisos se **simulan** escribiendo en el log (`[SIMULADO] ...`). El envío real de correo (con QR) queda como mejora futura.

## Qué hace

Escucha tres tópicos de Kafka y, por cada mensaje, avisa al usuario:

| Tópico | Quién lo publica | Qué avisa |
|---|---|---|
| `boleto.comprado` | ms-boletos | Correo de confirmación de la compra (con el comprobante / QR del boleto) |
| `compra.fallida` | ms-boletos | Aviso de que la compra no se pudo completar |
| `reserva.expirada` | ms-asientos | Aviso de que la reserva de asientos venció |

Además expone una API REST para crear, listar y eliminar tópicos de Kafka (ver "API de administración").

## Cómo encaja en la arquitectura

```
ms-boletos ──publica──► boleto.comprado ──┐
ms-boletos ──publica──► compra.fallida  ──┼──► Apache Kafka ──consume──► ms-notificaciones
ms-asientos ─publica──► reserva.expirada ─┘
```

- La **confirmación de los asientos** (pasar de RESERVADO a OCUPADO) sigue siendo una llamada directa de ms-boletos a ms-asientos. Kafka se usa solo para lo que es realmente asíncrono: los avisos.
- Si Kafka falla al publicar, la compra **no se cae**: el boleto ya quedó confirmado y el error solo se registra en el log.
- ms-notificaciones tiene su propia copia de los formatos de mensaje. Lo único que lo une con los otros servicios es el formato del mensaje.

## Tecnologías

- Java 21
- Spring Boot 4.1.1
- Spring for Apache Kafka
- Spring Web (para la API de administración)
- Jackson (para leer el JSON de los mensajes)
- Lombok
- Maven (incluye `mvnw`)

## Formato de los mensajes

Los mensajes viajan como texto JSON.

**`boleto.comprado`**

```json
{
  "codigoBoleto": "TF-AB12-CD34",
  "emailUsuario": "usuario@correo.cl",
  "tituloEvento": "Dune 3",
  "fechaHoraFuncion": "2026-10-18T20:00:00",
  "precioTotal": 11000,
  "asientos": ["F7", "F8"]
}
```

**`compra.fallida`**

```json
{
  "codigoBoleto": "TF-ZZ99-XX11",
  "emailUsuario": "usuario@correo.cl",
  "motivo": "La reserva expiró"
}
```

**`reserva.expirada`**

```json
{
  "funcionId": 5,
  "asientos": ["F7", "F8"],
  "emailUsuario": "usuario@correo.cl"
}
```

> Los campos de `reserva.expirada` deben coincidir con lo que publique ms-asientos. Si ese servicio cambia el formato, hay que ajustar `ReservaExpiradaEvent`.

## ¿De dónde viene el correo del usuario?

El campo `emailUsuario` de los eventos **no se pide de nuevo al usuario**: se obtiene del **JWT emitido por Cognito** al iniciar sesión (por ejemplo, con Google).

Flujo:

1. El usuario inicia sesión con Google → Cognito emite un JWT que incluye el claim `email`.
2. El frontend envía ese JWT en el header `Authorization` en cada llamada al backend.
3. `ms-boletos`, al recibir la petición autenticada, extrae el email del JWT y lo incluye en el evento `boleto.comprado`.
4. `ms-notificaciones` lee `emailUsuario` del JSON y lo usa como destinatario.

**ms-notificaciones no interactúa con Cognito.** Solo consume el campo del evento. Si el formato cambia, ajustar el DTO correspondiente.

## Estructura del proyecto

```
src/main/java/com/ticketfilms/ms_notificaciones/
├── config/
│   ├── JacksonConfig.java              # ObjectMapper explícito (JavaTimeModule)
│   ├── KafkaErrorHandlingConfig.java   # errores, reintentos, DLT, NewTopic
│   └── KafkaTopics.java                # nombres centralizados de tópicos y DLTs
├── controller/
│   └── KafkaAdminController.java       # API REST de administración
├── dto/
│   ├── BoletoCompradoEvent.java        # record
│   ├── CompraFallidaEvent.java         # record
│   └── ReservaExpiradaEvent.java       # record
├── exception/
│   └── MensajeInvalidoException.java   # error definitivo → DLT
├── listener/
│   └── NotificacionesListener.java     # un @KafkaListener por tópico
├── service/
│   └── KafkaAdminService.java          # lógica de administración de tópicos
└── MsNotificacionesApplication.java
```

## Configuración

Archivo `src/main/resources/application.properties`:

```properties
spring.application.name=ms-notificaciones

spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

# Consumer
spring.kafka.consumer.group-id=ms-notificaciones
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Producer (necesario para publicar en los tópicos DLT)
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
```

| Variable | Para qué sirve | Valor por defecto |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Dirección del broker de Kafka | `localhost:9092` |

No necesita base de datos, JWT ni otras variables de entorno.

## Cómo ejecutarlo en local

**1. Levantar Kafka** (con Docker Desktop abierto). El `docker-compose.yml` está en el repositorio de ms-boletos:

```bash
docker compose up -d
```

Espera unos 15 segundos a que Kafka termine de arrancar. Se puede comprobar con `docker ps` (el contenedor `kafka` debe estar en `Up`). Opcionalmente, la pantalla web de Kafka UI queda en http://localhost:8090.

**2. Levantar ms-notificaciones:**

```bash
./mvnw spring-boot:run
```

En el log deben aparecer las líneas `partitions assigned` para los tres tópicos. Eso significa que ya está escuchando.

## Cómo probarlo sin los otros servicios

Con ms-notificaciones corriendo, en otra terminal se pueden mandar mensajes de prueba. (En Git Bash en Windows, el prefijo `MSYS_NO_PATHCONV=1` evita que cambie la ruta `/opt/kafka/...`.)

```bash
echo '{"codigoBoleto":"TF-AB12-CD34","emailUsuario":"prueba@correo.cl","tituloEvento":"Dune 3","fechaHoraFuncion":"2026-10-18T20:00:00","precioTotal":11000,"asientos":["F7","F8"]}' | MSYS_NO_PATHCONV=1 docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic boleto.comprado
```

```bash
echo '{"codigoBoleto":"TF-ZZ99-XX11","emailUsuario":"prueba@correo.cl","motivo":"La reserva expiró"}' | MSYS_NO_PATHCONV=1 docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic compra.fallida
```

```bash
echo '{"funcionId":5,"asientos":["F7","F8"],"emailUsuario":"prueba@correo.cl"}' | MSYS_NO_PATHCONV=1 docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic reserva.expirada
```

En la consola de ms-notificaciones debería aparecer algo así:

```
[SIMULADO] Correo de confirmación a prueba@correo.cl - boleto TF-AB12-CD34 - Dune 3 - asientos [F7, F8] - total 11000
[SIMULADO] Aviso de compra fallida a prueba@correo.cl - boleto TF-ZZ99-XX11 - motivo: La reserva expiró
[SIMULADO] Aviso de reserva vencida a prueba@correo.cl - función 5 - asientos [F7, F8]
```

## Manejo de errores y reintentos

Los tres listeners usan un `DefaultErrorHandler` configurado en `KafkaErrorHandlingConfig`:

| Situación | Comportamiento |
|---|---|
| Procesamiento exitoso | Se confirma el offset automáticamente. |
| Error recuperable (ej: servicio externo no responde) | Se reintenta 3 veces con backoff exponencial: 1s, 2s, 4s. Si se agotan, va al DLT. |
| Error definitivo (`MensajeInvalidoException`: JSON malformado o incompleto) | No se reintenta. Va directo al DLT. |

Los mensajes fallidos se publican en `<topico>.DLT` (ej: `boleto.comprado.DLT`) y se registran en el log con tópico, partición, offset y motivo.

### Evidencia de prueba

**Mensaje válido** → procesado:

```
[SIMULADO] Correo de confirmación a a@b.cl - boleto TF-AB12-CD34 - Dune 3 - asientos [F7, F8] - total 11000
```

**Mensaje inválido** (`esto-no-es-json`) → al DLT sin reintentos:

```
ERROR ... Mensaje enviado a DLT — topico=boleto.comprado particion=0 offset=1
  motivo=Listener method ... threw exception mensaje=esto-no-es-json
```

Verificación con `kafka-console-consumer` sobre `boleto.comprado.DLT`:

```
esto-no-es-json
Processed a total of 1 messages
```

## API de administración de tópicos

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/api/admin/kafka/topics` | Lista todos los tópicos existentes |
| POST | `/api/admin/kafka/topics` | Crea un tópico (valida nombre, particiones, réplicas) |
| DELETE | `/api/admin/kafka/topics/{nombre}` | Elimina un tópico (no permite borrar tópicos del sistema) |

**Validaciones aplicadas:**

- Nombre: solo `[a-zA-Z0-9._-]`, longitud 1..249
- Particiones: 1..100
- Réplicas: 1..10
- Tópicos protegidos (`boleto.comprado`, `compra.fallida`, `reserva.expirada` y sus DLT) no se pueden eliminar

### Ejemplos

```bash
# Listar
curl http://localhost:8080/api/admin/kafka/topics

# Crear
curl -X POST http://localhost:8080/api/admin/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{"nombre":"test.topic","particiones":1,"replicas":1}'

# Nombre inválido → 400
curl -X POST http://localhost:8080/api/admin/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{"nombre":"con espacios","particiones":1,"replicas":1}'

# Borrar tópico protegido → 400
curl -X DELETE http://localhost:8080/api/admin/kafka/topics/boleto.comprado
```

## Pruebas

El proyecto incluye tests unitarios del listener con Mockito:

```bash
./mvnw test
```

Cubren:

- Procesamiento correcto de mensajes válidos para los 3 tópicos.
- Lanzamiento de `MensajeInvalidoException` con JSON malformado.

## Errores comunes

| Síntoma | Causa probable |
|---|---|
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka no está corriendo. Revisar Docker Desktop y `docker ps`. |
| `This is not the correct coordinator` al arrancar | Kafka recién se encendió y aún no está listo. Se resuelve solo en unos segundos. |
| `No qualifying bean of type 'KafkaAdmin'` | En Spring Boot 4.x, la autoconfiguración de Kafka no se activa sola. Los beans `ProducerFactory`, `KafkaTemplate`, `KafkaAdmin` y `ObjectMapper` están declarados explícitamente en `KafkaErrorHandlingConfig` y `JacksonConfig`. |
| `Mensaje inválido en ...` | El JSON del mensaje no coincide con el formato esperado. El log indica el motivo. |

## Mejoras pendientes

- Enviar el correo real con QR usando `spring-boot-starter-mail` + ZXing. Para pruebas locales se puede usar Mailpit en Docker.
- Adjuntar el PDF del boleto además del QR incrustado en el HTML.
- Agregar un health check (`spring-boot-starter-actuator`) para monitoreo.
- Desplegar junto al resto de los servicios en EC2.
```
