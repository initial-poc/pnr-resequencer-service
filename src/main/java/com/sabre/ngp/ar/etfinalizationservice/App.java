package com.sabre.ngp.ar.etfinalizationservice;

import com.sabre.ngp.ar.etfinalizationservice.component.OutboxPollerExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;

@SpringBootApplication
@EnableScheduling
public class App implements CommandLineRunner {
	@Autowired
	private OutboxPollerExecutor outboxPollerExecutor;
	public static void main(String[] args) throws IOException {
		SpringApplication.run(App.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		outboxPollerExecutor.process();
	}
}