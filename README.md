# ms-notificaciones

Microservicio de **TicketFilms** que avisa por correo a los usuarios cuando pasa algo con sus compras o reservas. No tiene base de datos: **escucha mensajes de Apache Kafka**, envía el correo correspondiente y expone una **API de administración** (protegida con Cognito) para gestionar tópicos.

## Qué hace

Escucha tres tópicos de Kafka. Por cada mensaje válido envía un correo real (SMTP, probado con Postmark):

| Tópico | Quién lo publica | Consumidor | Qué hace |
|---|---|---|---|
| `boleto.comprado` | ms-boletos | `NotificacionesListener.consumirBoletoComprado` (ms-notificaciones) | Correo de confirmación con el código del boleto y un QR |
| `compra.fallida` | ms-boletos | `NotificacionesListener.consumirCompraFallida` (ms-notificaciones) | Correo de que la compra no se pudo completar |
| `compra.fallida` | ms-boletos | `AsientoKafkaConsumer` (ms-asientos, grupo distinto) | Libera los asientos reservados por ese usuario |
| `reserva.expirada` | ms-asientos | `NotificacionesListener.consumirReservaExpirada` (ms-notificaciones) | Correo de que la reserva de 5 minutos venció |

`compra.fallida` es la regla de distribución con dos destinatarios: cada servicio lee el mismo mensaje con su propio grupo de consumidores (`ms-notificaciones` y `ms-asientos-group`), así que ambos reciben una copia y cada uno usa solo los campos que le sirven.

## Cómo encaja en la arquitectura

```
ms-boletos ──publica──► boleto.comprado  ──────────────► ms-notificaciones (correo + QR)
ms-boletos ──publica──► compra.fallida   ──┬───────────► ms-notificaciones (correo)
                                           └───────────► ms-asientos (libera asientos)
ms-asientos ─publica──► reserva.expirada ──────────────► ms-notificaciones (correo)
```

- La **confirmación de los asientos** (RESERVADO a OCUPADO) sigue siendo una llamada directa de ms-boletos a ms-asientos. Kafka se usa solo para lo asíncrono: los avisos y la liberación por falla.
- Si Kafka falla al publicar, la compra o la reserva **no se caen**: solo se registra el error en el log.
- Cada servicio tiene su propia copia de los formatos de mensaje. Lo único que los une es el formato del mensaje.

## Tecnologías

- Java 21
- Spring Boot 4.1.1
- Spring for Apache Kafka
- Spring Web (API de administración)
- Spring Security + OAuth2 Resource Server (JWT de Cognito)
- Spring Mail (envío por SMTP) y ZXing (imagen del QR)
- Jackson, Lombok
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

**`compra.fallida`** (los campos `funcionId`, `asientosIds` y `usuarioId` los usa ms-asientos; ms-notificaciones los ignora)

```json
{
  "codigoBoleto": "TF-ZZ99-XX11",
  "emailUsuario": "usuario@correo.cl",
  "motivo": "La reserva expiró",
  "funcionId": 5,
  "asientosIds": [1, 2],
  "usuarioId": "sub-del-usuario"
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

## ¿De dónde viene el correo del usuario?

El campo `emailUsuario` no se pide de nuevo al usuario: sale del login con **Cognito**.

1. El usuario inicia sesión con Google y Cognito emite un JWT.
2. El frontend envía el JWT en el header `Authorization`.
3. ms-boletos y ms-asientos obtienen el email (del claim `email` o, si falta, del endpoint userInfo de Cognito) y lo incluyen en el evento.
4. ms-notificaciones lee `emailUsuario` del JSON y lo usa como destinatario.

Para los eventos, ms-notificaciones no habla con Cognito. Solo valida el JWT de quien usa la API de administración.

## Estructura del proyecto

```
src/main/java/com/ticketfilms/ms_notificaciones/
├── config/
│   ├── JacksonConfig.java              # ObjectMapper explícito (JavaTimeModule)
│   ├── KafkaErrorHandlingConfig.java   # errores, reintentos, DLT, NewTopic
│   ├── KafkaTopics.java                # nombres de tópicos y DLT, un solo lugar
│   └── SecurityConfig.java             # JWT de Cognito, /api/admin/** solo grupo admin
├── controller/
│   └── KafkaAdminController.java       # API REST de administración
├── dto/
│   ├── BoletoCompradoEvent.java        # eventos (records)
│   ├── CompraFallidaEvent.java
│   ├── ReservaExpiradaEvent.java
│   ├── TopicoDto.java                  # respuestas de la API admin
│   └── GrupoConsumidorDto.java
├── exception/
│   ├── GlobalExceptionHandler.java     # traduce errores a respuestas HTTP
│   ├── MensajeInvalidoException.java   # error definitivo de un mensaje -> DLT
│   ├── TopicoNoEncontradoException.java
│   └── TopicoYaExisteException.java
├── listener/
│   └── NotificacionesListener.java     # un @KafkaListener por tópico
├── service/
│   ├── EmailService.java               # arma y envía los correos
│   ├── QrService.java                  # genera la imagen del QR
│   └── KafkaAdminService.java          # operaciones y validación de tópicos
└── MsNotificacionesApplication.java
```

Separación de responsabilidades: los listeners validan el mensaje y delegan; `EmailService` y `QrService` saben cómo armar el correo; `KafkaErrorHandlingConfig` decide qué pasa cuando algo falla. Ninguna clase mezcla esos roles.

## Configuración

Los nombres de tópicos están en `KafkaTopics` y el grupo de consumidores en `application.properties` (los listeners no repiten ninguno de los dos). Archivo `src/main/resources/application.properties`:

```properties
spring.application.name=ms-notificaciones
server.port=${SERVER_PORT:8084}

spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

# Consumer: este group-id lo usan todos los @KafkaListener
spring.kafka.consumer.group-id=ms-notificaciones
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Producer (necesario para publicar en los tópicos DLT)
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# Correo
spring.mail.host=${MAIL_HOST:localhost}
spring.mail.port=${MAIL_PORT:1025}
spring.mail.username=${MAIL_USERNAME:}
spring.mail.password=${MAIL_PASSWORD:}
spring.mail.properties.mail.smtp.auth=${MAIL_SMTP_AUTH:false}
spring.mail.properties.mail.smtp.starttls.enable=${MAIL_STARTTLS:false}
app.mail.from=${MAIL_FROM:no-reply@ticketfilms.local}

# Seguridad de la API admin
spring.security.oauth2.resourceserver.jwt.issuer-uri=${COGNITO_ISSUER_URI}
app.admin.group=${ADMIN_GROUP:admin}
```

| Variable | Para qué sirve | Valor por defecto |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Dirección del broker de Kafka | `localhost:9092` |
| `SERVER_PORT` | Puerto del servicio (8080 lo usa el API Manager) | `8084` |
| `MAIL_HOST` / `MAIL_PORT` | Servidor SMTP | `localhost` / `1025` |
| `MAIL_USERNAME` / `MAIL_PASSWORD` | Credenciales SMTP (en Postmark, el token va en ambas) | vacío |
| `MAIL_SMTP_AUTH` / `MAIL_STARTTLS` | Autenticación y STARTTLS del SMTP | `false` |
| `MAIL_FROM` | Remitente; en Postmark debe ser una Sender Signature verificada | `no-reply@ticketfilms.local` |
| `COGNITO_ISSUER_URI` | Emisor del JWT de Cognito (obligatoria) | sin valor por defecto |
| `ADMIN_GROUP` | Grupo de Cognito autorizado en la API admin | `admin` |

Las variables van en un archivo `.env` (ya está en `.gitignore`) y se cargan con `export $(grep -v '^#' .env | xargs)`.

## Cómo ejecutarlo en local

**1. Levantar Kafka** (con Docker Desktop abierto). El `docker-compose.yml` está en el repositorio de ms-boletos:

```bash
docker compose up -d
```

Espera unos 15 segundos. Se puede comprobar con `docker ps` (contenedor `kafka` en `Up`). Opcionalmente, Kafka UI queda en http://localhost:8090.

**2. Levantar ms-notificaciones:**

```bash
export $(grep -v '^#' .env | xargs)
mvn spring-boot:run
```

En el log deben aparecer las líneas `partitions assigned` para los tres tópicos. Eso significa que ya está escuchando.

## Cómo probarlo sin los otros servicios

Con ms-notificaciones corriendo, en otra terminal se pueden mandar mensajes de prueba. (En Git Bash en Windows, `MSYS_NO_PATHCONV=1` evita que cambie la ruta `/opt/kafka/...`.)

```bash
echo '{"codigoBoleto":"TF-AB12-CD34","emailUsuario":"tu@correo.cl","tituloEvento":"Dune 3","fechaHoraFuncion":"2026-10-18T20:00:00","precioTotal":11000,"asientos":["F7","F8"]}' | MSYS_NO_PATHCONV=1 docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic boleto.comprado
```

```bash
echo '{"codigoBoleto":"TF-ZZ99-XX11","emailUsuario":"tu@correo.cl","motivo":"La reserva expiró","funcionId":5,"asientosIds":[1,2],"usuarioId":"sub-de-prueba"}' | MSYS_NO_PATHCONV=1 docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic compra.fallida
```

```bash
echo '{"funcionId":5,"asientos":["F7","F8"],"emailUsuario":"tu@correo.cl"}' | MSYS_NO_PATHCONV=1 docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic reserva.expirada
```

Cada mensaje válido envía un correo y deja una línea en el log, por ejemplo:

```
Correo de reserva expirada enviado a tu@correo.cl - función 5
```

## Manejo de errores y reintentos

Los tres listeners usan un `DefaultErrorHandler` configurado en `KafkaErrorHandlingConfig`:

| Situación | Comportamiento |
|---|---|
| Procesamiento exitoso | Se confirma el offset. |
| Error recuperable (ej: el servidor SMTP no responde) | Se reintenta 3 veces con espera exponencial: 1s, 2s, 4s. Si se agotan, va al DLT. |
| Error definitivo (`MensajeInvalidoException`: JSON malformado o campo obligatorio faltante) | No se reintenta. Va directo al DLT. |

Los mensajes fallidos se publican en `<topico>.DLT` (ej: `boleto.comprado.DLT`) y se registran en el log con tópico, partición, offset y motivo.

Un `compra.fallida` sin email no es un mensaje inválido: ms-asientos igual necesita ese mensaje para liberar asientos. Se registra un aviso y se omite solo el correo.

### Cómo reproducirlo

- **Éxito:** cualquiera de los mensajes de la sección anterior.
- **Error recuperable:** arrancar con un SMTP inexistente (`MAIL_HOST=localhost MAIL_PORT=1 mvn spring-boot:run`) y mandar un mensaje válido. El log muestra `Reintento 1 de 3`, `2 de 3`, `3 de 3` y luego `Mensaje enviado a DLT`.
- **Error definitivo:** `echo 'esto-no-es-json' | ... --topic reserva.expirada`. Va al DLT sin líneas de reintento.
- **Ver el DLT:**

```bash
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reserva.expirada.DLT --from-beginning --timeout-ms 5000
```

## API de administración

Requiere un JWT de Cognito de un usuario del grupo `admin` (`Authorization: Bearer <access_token>`). Sin token responde 401; con un usuario de otro grupo, 403. A través del API Manager se llama como `/api/admin/kafka/...` (puerto 8080); directo al servicio, en el puerto 8084.

| Método | Endpoint | Descripción | Respuestas |
|---|---|---|---|
| GET | `/api/admin/kafka/topics` | Lista los tópicos | 200 |
| GET | `/api/admin/kafka/topics/{nombre}` | Detalle: particiones, réplicas, si es protegido | 200, 404 |
| POST | `/api/admin/kafka/topics` | Crea un tópico | 201, 400, 409 |
| DELETE | `/api/admin/kafka/topics/{nombre}` | Elimina un tópico | 200, 400, 404 |
| GET | `/api/admin/kafka/consumer-groups` | Grupos de consumidores y los tópicos que escuchan | 200 |

**Validaciones:**

- `@Valid` en el cuerpo: nombre obligatorio, particiones 1..100, réplicas 1..10.
- Nombre: solo `[a-zA-Z0-9._-]`, longitud 1..249; se rechazan `.` y `..`.
- Un tópico existente no se vuelve a crear (409). Consultar o borrar uno inexistente da 404.
- Los tópicos del sistema (`boleto.comprado`, `compra.fallida`, `reserva.expirada` y sus DLT) no se pueden eliminar.

Los errores tienen el mismo formato: `timestamp`, `status`, `error`, `mensaje`.

### Ejemplos

```bash
URL=http://localhost:8084/api/admin/kafka
TOKEN="access_token_de_un_usuario_admin"

# Listar
curl -H "Authorization: Bearer $TOKEN" $URL/topics

# Detalle
curl -H "Authorization: Bearer $TOKEN" $URL/topics/boleto.comprado

# Crear -> 201
curl -X POST $URL/topics -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"nombre":"test.topic","particiones":1,"replicas":1}'

# Nombre inválido -> 400
curl -X POST $URL/topics -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"nombre":"con espacios","particiones":1,"replicas":1}'

# Borrar tópico protegido -> 400
curl -X DELETE -H "Authorization: Bearer $TOKEN" $URL/topics/boleto.comprado

# Grupos de consumidores
curl -H "Authorization: Bearer $TOKEN" $URL/consumer-groups
```

## Pruebas

```bash
./mvnw test
```

Los tests del listener (Mockito) cubren, para los tres tópicos: mensaje válido (se envía el correo), JSON inválido y campos obligatorios faltantes (`MensajeInvalidoException`, sin enviar correo), y el caso de `compra.fallida` sin email (se omite sin error).

## Errores comunes

| Síntoma | Causa probable |
|---|---|
| `Port 8084 was already in use` | Hay otra instancia corriendo. Cerrarla o usar `SERVER_PORT=otro mvn spring-boot:run`. |
| `Could not resolve placeholder 'COGNITO_ISSUER_URI'` | Falta la variable. Agregarla al `.env` y cargarlo antes de arrancar. |
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka no está corriendo. Revisar Docker Desktop y `docker ps`. |
| Los mensajes terminan en el DLT tras 3 reintentos | El SMTP no responde o el remitente (`MAIL_FROM`) no está verificado en Postmark. |
| 401 en la API admin | Falta el token, expiró (duran 1 hora) o es el `id_token` en vez del `access_token`. |
| 403 en la API admin | El usuario no pertenece al grupo `admin` de Cognito. |
| `Unresolved compilation problems` al arrancar | VS Code compiló en `target/` con otro nivel de Java. Correr `mvn clean spring-boot:run`. |

## Mejoras pendientes

- Adjuntar el PDF del boleto además del QR incrustado en el HTML.
- Agregar un health check (`spring-boot-starter-actuator`) para monitoreo.
- Desplegar junto al resto de los servicios en EC2 y exponer la API admin en el API Manager (ruta `/api/admin/**` hacia el puerto 8084).
