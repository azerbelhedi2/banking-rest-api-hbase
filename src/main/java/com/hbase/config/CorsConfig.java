package com.hbase.config;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
            .allowedOrigins("http://localhost:4200","http://192.168.1.150","http://localhost")
            .allowedMethods("GET","POST","PUT","DELETE")
            .allowedHeaders("*")
            .allowCredentials(true);
    }
}
