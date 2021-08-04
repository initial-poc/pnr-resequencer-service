package com.sabre.ngp.ar.etfinalizationservice;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.sabre.ngp.ar.etfinalizationservice.component.OutboxPollerExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class GenericSpringBootApplication implements CommandLineRunner {

    @Autowired
    private OutboxPollerExecutor executor;
    public static void main(String[] args) throws Exception{
        Stopwatch s= Stopwatch.createStarted();
        TimeUnit.MINUTES.sleep(1);
        System.out.println(s.stop());
   //   SpringApplication.run(GenericSpringBootApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        executor.process();
    }
}
