package com.topcoder.or;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@ImportResource("classpath:searchBundle.xml")
@SpringBootApplication()
public class OnlineReviewGrpcServer {
    public static void main(String[] args) {
        SpringApplication.run(OnlineReviewGrpcServer.class, args);
    }
}
