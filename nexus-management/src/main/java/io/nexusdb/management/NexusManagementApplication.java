package io.nexusdb.management;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NexusManagementApplication {

    public static void main(String[] args) {
        SpringApplication.run(NexusManagementApplication.class, args);
    }
}
