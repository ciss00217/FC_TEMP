package com.ibm.tfb.ext;

import com.ibm.dpft.engine.core.EngineExecutor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@EnableAutoConfiguration(exclude = {org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class})
@SpringBootApplication
public class ExtApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(ExtApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		EngineExecutor.startAutomation(args);
	}
}
