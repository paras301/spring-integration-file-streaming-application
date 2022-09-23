package com.company;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Sample class to store app props.
 */
@Configuration
@ConfigurationProperties(prefix = "app")
@Data
public class Config {
    private String csv_input_dir;
    private String csv_file_pattern;
    private String csv_processing_dir;
    private String csv_success_dir;
    private String csv_error_dir;
    private String csv_output_dir;

}
