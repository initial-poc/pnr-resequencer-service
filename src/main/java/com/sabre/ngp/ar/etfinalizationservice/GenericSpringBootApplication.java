package com.sabre.ngp.ar.etfinalizationservice;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.sabre.ngp.ar.etfinalizationservice.component.MessagePublisher;
import com.sabre.ngp.ar.etfinalizationservice.component.OutboxPollerExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GenericSpringBootApplication implements CommandLineRunner {

    @Autowired
    private OutboxPollerExecutor executor;
    public static void main(String[] args) throws Exception{
     SpringApplication.run(GenericSpringBootApplication.class, args);

    }

    @Override
    public void run(String... args) throws Exception {
   //     executor.process();
    }


}
