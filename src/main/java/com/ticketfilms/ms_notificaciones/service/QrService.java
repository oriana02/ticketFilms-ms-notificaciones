package com.ticketfilms.ms_notificaciones.service;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.springframework.stereotype.Service;

// Solo genera la imagen del QR. No sabe nada de correos ni de Kafka.
@Service
public class QrService {

    private static final int TAMANO = 300;

    public byte[] generarPng(String contenido) {
        try {
            BitMatrix matriz = new QRCodeWriter()
                    .encode(contenido, BarcodeFormat.QR_CODE, TAMANO, TAMANO);
            ByteArrayOutputStream salida = new ByteArrayOutputStream();
            MatrixToImageWriter.writeToStream(matriz, "PNG", salida);
            return salida.toByteArray();
        } catch (WriterException | IOException e) {
            throw new IllegalStateException("No se pudo generar el QR", e);
        }
    }
}
