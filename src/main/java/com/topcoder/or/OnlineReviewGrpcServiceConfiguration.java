package com.topcoder.or;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@ComponentScan("com.topcoder")
public class OnlineReviewGrpcServiceConfiguration {
    @Bean
    @Scope("prototype")
    public Logger produceLogger(InjectionPoint injectionPoint) {
        return org.slf4j.LoggerFactory.getLogger(injectionPoint.getMember().getDeclaringClass());
    }

    @Bean(name = "db1")
    @ConfigurationProperties(prefix = "spring.datasource1")
    public DataSource dataSource1() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "tcsJdbcTemplate")
    public JdbcTemplate jdbcTemplate1(@Qualifier("db1") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean(name = "db2")
    @ConfigurationProperties(prefix = "spring.datasource2")
    public DataSource dataSource2() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "oltpJdbcTemplate")
    public JdbcTemplate jdbcTemplate2(@Qualifier("db2") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean(name = "db3")
    @ConfigurationProperties(prefix = "spring.datasource3")
    public DataSource dataSource3() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "tcsDwJdbcTemplate")
    public JdbcTemplate jdbcTemplate3(@Qualifier("db3") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean(name = "db4")
    @ConfigurationProperties(prefix = "spring.datasource4")
    public DataSource dataSource4() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "commonJdbcTemplate")
    public JdbcTemplate jdbcTemplate4(@Qualifier("db4") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
