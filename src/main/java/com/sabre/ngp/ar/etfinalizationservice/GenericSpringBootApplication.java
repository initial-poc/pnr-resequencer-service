package com.sabre.ngp.ar.etfinalizationservice;

import com.sabre.ngp.ar.etfinalizationservice.component.OutboxPollerExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.SortedSet;

@SpringBootApplication
@EnableAsync
public class GenericSpringBootApplication implements CommandLineRunner {

    @Autowired
    private OutboxPollerExecutor executor;
    public static void main(String[] args) {

        SpringApplication.run(GenericSpringBootApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        executor.process();
    }
}
