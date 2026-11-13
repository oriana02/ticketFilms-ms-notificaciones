# ms-notificaciones

Microservicio de **TicketFilms** encargado exclusivamente del **consumo de eventos** y el **envío de correos electrónicos** transaccionales (con generación de códigos QR para los boletos). 

**Nota de arquitectura:** En versiones anteriores, este servicio administraba la infraestructura de RabbitMQ (creando/borrando colas). Esa responsabilidad fue **migrada a `ms-admin-mensajeria`**. Hoy, `ms-notificaciones` es un "worker" puro: no expone APIs HTTP, no tiene servidor web (Tomcat) y se dedica al 100% a escuchar mensajes y enviar correos.

## Qué hace

Escucha las colas de RabbitMQ. Por cada mensaje válido envía un correo real (SMTP, probado con Postmark):

| Cola (RabbitMQ) | Quién lo publica | Consumidor | Qué hace |
|---|---|---|---|
| `q.boleto.comprado.notificaciones` | ms-boletos | `NotificacionesListener.consumirBoletoComprado` | Correo de confirmación con el código del boleto y un QR |
| `q.compra.fallida.notificaciones` | ms-boletos | `NotificacionesListener.consumirCompraFallida` | Correo de que la compra no se pudo completar |
| `q.reserva.expirada.notificaciones` | ms-asientos | `NotificacionesListener.consumirReservaExpirada` | Correo de que la reserva de 5 minutos venció |

## Cómo encaja en la arquitectura

```
ms-boletos ──publica──► ex.tickets.eventos (topic) ──► q.boleto.comprado.notificaciones ──► ms-notificaciones (correo + QR)
ms-boletos ──publica──► ex.tickets.eventos (topic) ──► q.compra.fallida.notificaciones ──► ms-notificaciones (correo)
                                                    └──► q.compra.fallida.asientos ──────► ms-asientos (libera asientos)
ms-asientos ─publica──► ex.tickets.eventos (topic) ──► q.reserva.expirada.notificaciones ─► ms-notificaciones (correo)
```

- La **confirmación de los asientos** (RESERVADO a OCUPADO) sigue siendo una llamada directa de ms-boletos a ms-asientos (REST).
- RabbitMQ se usa para tareas de un solo consumidor (notificaciones) con reintentos y DLQ.
- **Kafka** se usa en paralelo en la arquitectura para el flujo de eventos (analítica), pero no es responsabilidad de este microservicio.

## Tecnologías

- Java 21
- Spring Boot 4.x
- Spring for RabbitMQ (Spring AMQP)
- Spring Mail (envío por SMTP) y ZXing (imagen del QR)
- Thymeleaf (plantillas HTML para los correos)
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
3. ms-boletos y ms-asientos obtienen el email del token y lo incluyen en el evento antes de publicar en Rabbit.
4. ms-notificaciones lee `emailUsuario` del JSON y lo usa como destinatario.

## Estructura del proyecto

```
src/main/java/com/ticketfilms/ms_notificaciones/
├── config/
│   ├── JacksonConfig.java              # ObjectMapper explícito (JavaTimeModule)
│   ├── RabbitTopology.java             # Nombres de colas, exchanges y DLQs (constantes)
│   └── RabbitTopologyConfig.java       # Declaración idempotente de la topología al arrancar
├── dto/
│   ├── BoletoCompradoEvent.java        # eventos (records)
│   ├── CompraFallidaEvent.java
│   └── ReservaExpiradaEvent.java
├── exception/
│   └── MensajeInvalidoException.java   # error definitivo -> DLQ
├── listener/
│   ├── NotificacionesListener.java     # @RabbitListener por cada cola
│   ├── ProcesadorAck.java              # Manejo manual de ACK y reintentos hacia DLQ
│   └── DlqListener.java                # (Opcional) Monitorea las DLQs
├── service/
│   ├── EmailService.java               # arma y envía los correos
│   └── QrService.java                  # genera la imagen del QR
└── MsNotificacionesApplication.java
```

Separación de responsabilidades: los listeners validan el mensaje y delegan; `EmailService` sabe cómo armar el correo; `ProcesadorAck` decide qué pasa cuando algo falla.

## Configuración

Archivo `src/main/resources/application.properties`:

```properties
spring.application.name=ms-notificaciones
server.port=${SERVER_PORT:8084}

# RabbitMQ
spring.rabbitmq.host=${RABBITMQ_HOST:localhost}
spring.rabbitmq.port=${RABBITMQ_PORT:5672}
spring.rabbitmq.username=${RABBITMQ_USER:guest}
spring.rabbitmq.password=${RABBITMQ_PASSWORD:guest}
spring.rabbitmq.listener.simple.acknowledge-mode=manual
spring.rabbitmq.listener.simple.default-requeue-rejected=false
spring.rabbitmq.listener.simple.prefetch=1

# Correo
spring.mail.host=${MAIL_HOST:localhost}
spring.mail.port=${MAIL_PORT:1025}
spring.mail.username=${MAIL_USERNAME:}
spring.mail.password=${MAIL_PASSWORD:}
spring.mail.properties.mail.smtp.auth=${MAIL_SMTP_AUTH:false}
spring.mail.properties.mail.smtp.starttls.enable=${MAIL_STARTTLS:false}
app.mail.from=${MAIL_FROM:no-reply@ticketfilms.local}
```

| Variable | Para qué sirve | Valor por defecto |
|---|---|---|
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | Dirección del broker | `localhost` / `5672` |
| `SERVER_PORT` | Puerto (aunque no expone API) | `8084` |
| `MAIL_HOST` / `MAIL_PORT` | Servidor SMTP | `localhost` / `1025` |
| `MAIL_USERNAME` / `MAIL_PASSWORD` | Credenciales SMTP | vacío |
| `MAIL_FROM` | Remitente verificado en Postmark | `no-reply@ticketfilms.local` |

## Manejo de errores y reintentos

Los listeners usan un `ProcesadorAck` que maneja el ACK manual:

| Situación | Comportamiento |
|---|---|
| Procesamiento exitoso | Se envía el `basicAck`. |
| Error recuperable (ej: el SMTP no responde) | Se reintenta 3 veces con espera exponencial: 1s, 2s, 4s. Si se agotan, el mensaje se envía a la DLQ. |
| Error definitivo (`MensajeInvalidoException`: JSON malformado) | No se reintenta. Va directo a la DLQ. |

Los mensajes fallidos se publican en la DLQ (`<nombre-cola>.dlq`) y se registran en el log con el contexto del fallo.

## Mejoras pendientes

- Adjuntar el PDF del boleto además del QR incrustado en el HTML.
- Agregar un health check (`spring-boot-starter-actuator`) si se requiere monitoreo en la nube.
- Desplegar junto al resto de los servicios en EC2.
```
