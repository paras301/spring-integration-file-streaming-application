package com.company;


import com.company.vo.MappingConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.io.InputStream;

@SpringBootApplication(scanBasePackages = {"com.company.**.**"})
@Slf4j
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        log.info("Application Started...");
    }

    @Bean
    @Qualifier("orderMap")
    public MappingConfiguration mappingConfiguration1() throws IOException {
        ObjectMapper om = new ObjectMapper();
        InputStream is = getClass().getResourceAsStream("/orderMap.json");
        return om.readValue(is, MappingConfiguration.class);
    }
}
