package com.ticketfilms.ms_notificaciones.config;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;

// La API de administración de Kafka solo la puede usar un usuario autenticado
// con Cognito que pertenezca al grupo de administradores (app.admin.group).
//  - Sin token o token inválido -> 401
//  - Token válido pero sin el grupo admin -> 403
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);
    private static final String CLAIM_GRUPOS = "cognito:groups";
    private static final String PREFIJO_GRUPO = "GRUPO_";

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http,
            @Value("${app.admin.group:admin}") String grupoAdmin) throws Exception {
        http
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/admin/**").hasAuthority(PREFIJO_GRUPO + grupoAdmin)
                .anyRequest().denyAll())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt
                .jwtAuthenticationConverter(convertidor())))
                .exceptionHandling(ex -> ex
                .authenticationEntryPoint((request, response, e) -> {
                    log.warn("401 no autenticado — path={} motivo={}", request.getRequestURI(), e.getMessage());
                    new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED).commence(request, response, e);
                })
                .accessDeniedHandler((request, response, e) -> {
                    log.warn("403 acceso denegado — path={} motivo={}", request.getRequestURI(), e.getMessage());
                    response.setStatus(HttpStatus.FORBIDDEN.value());
                }));
        return http.build();
    }

    // Convierte cada grupo de Cognito en una autoridad: "admin" -> GRUPO_admin
    private JwtAuthenticationConverter convertidor() {
        JwtAuthenticationConverter convertidor = new JwtAuthenticationConverter();
        convertidor.setJwtGrantedAuthoritiesConverter(jwt -> {
            List<String> grupos = jwt.getClaimAsStringList(CLAIM_GRUPOS);
            if (grupos == null) {
                return List.<GrantedAuthority>of();
            }
            return grupos.stream()
                    .<GrantedAuthority>map(g -> new SimpleGrantedAuthority(PREFIJO_GRUPO + g))
                    .toList();
        });
        return convertidor;
    }
}
