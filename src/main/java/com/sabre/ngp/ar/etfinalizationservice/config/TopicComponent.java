package com.sabre.ngp.ar.etfinalizationservice.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "topic")
@Data
public class TopicComponent {
    private Map<String, String> name = new HashMap<>();
}
