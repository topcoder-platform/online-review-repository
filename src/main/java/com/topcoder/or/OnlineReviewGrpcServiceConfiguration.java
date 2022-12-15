package com.topcoder.or;

import org.slf4j.Logger;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@ComponentScan("com.topcoder")
public class OnlineReviewGrpcServiceConfiguration {
    @Bean
    @Scope("prototype")
    public Logger produceLogger(InjectionPoint injectionPoint) {
        return org.slf4j.LoggerFactory.getLogger(injectionPoint.getMember().getDeclaringClass());
    }
}
