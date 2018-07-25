package com.busybox.kafka.twitter.producer.application;

import com.busybox.kafka.twitter.producer.configuration.ApplicationConfiguration;
import com.busybox.kafka.twitter.producer.service.TwitterProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;

@Slf4j
@SpringBootApplication
public class Application extends SpringBootServletInitializer {

    private static final Class[] PRIMARY_SOURCES = {Application.class, ApplicationConfiguration.class};

    @Override
    protected SpringApplicationBuilder configure(final SpringApplicationBuilder builder) {
        return builder.sources(PRIMARY_SOURCES);
    }

    public static void main(String[] args) {
        final ConfigurableApplicationContext applicationContext = SpringApplication.run(PRIMARY_SOURCES, args);

        applicationContext.getBean(TwitterProducerService.class).run();
    }
}
