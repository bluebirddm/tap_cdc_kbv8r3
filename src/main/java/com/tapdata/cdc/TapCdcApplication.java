package com.tapdata.cdc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TapCdcApplication {
    public static void main(String[] args) {
        SpringApplication.run(TapCdcApplication.class, args);
    }
}
