# ms-notificaciones

Microservicio de **TicketFilms** que avisa a los usuarios cuando pasa algo con sus compras o reservas. No tiene base de datos ni endpoints HTTP: solo **escucha mensajes de Apache Kafka** y reacciona a ellos.

> Estado actual: los avisos se **simulan** escribiendo en el log (`[SIMULADO] ...`). El envío real de correo (con QR) queda como mejora futura.

## Qué hace

Escucha tres tópicos de Kafka y, por cada mensaje, avisa al usuario:

| Tópico | Quién lo publica | Qué avisa |
|---|---|---|
| `boleto.comprado` | ms-boletos | Correo de confirmación de la compra (con el comprobante / QR del boleto) |
| `compra.fallida` | ms-boletos | Aviso de que la compra no se pudo completar |
| `reserva.expirada` | ms-asientos | Aviso de que la reserva de asientos venció |

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

## Estructura del proyecto

```
src/main/java/com/ticketfilms/ms_notificaciones/
├── config/
│   └── KafkaTopicConfig.java        # crea los tópicos si no existen
├── dto/
│   ├── BoletoCompradoEvent.java
│   ├── CompraFallidaEvent.java
│   └── ReservaExpiradaEvent.java
└── listener/
    └── NotificacionesListener.java  # un @KafkaListener por tópico
```

## Configuración

Archivo `src/main/resources/application.properties`:

```properties
spring.application.name=ms-notificaciones

spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
spring.kafka.consumer.group-id=ms-notificaciones
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
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

## Errores comunes

| Síntoma | Causa probable |
|---|---|
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka no está corriendo. Revisar Docker Desktop y `docker ps`. |
| `This is not the correct coordinator` al arrancar | Kafka recién se encendió y aún no está listo. Se resuelve solo en unos segundos. |
| `Mensaje inválido en ...` | El JSON del mensaje no coincide con el formato esperado. El log indica el motivo. |

## Mejoras pendientes

- Enviar el correo real (`spring-boot-starter-mail`; para pruebas locales se puede usar Mailpit en Docker).
- Generar el código QR del boleto (librería ZXing).
- Agregar un health check (`spring-boot-starter-actuator`) para monitoreo.
- Desplegar junto al resto de los servicios en EC2.
