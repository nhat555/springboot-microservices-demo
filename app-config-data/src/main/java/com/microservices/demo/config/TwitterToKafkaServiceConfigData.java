package com.microservices.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {
    private List<String> twitterKeywords;
    private String welcomeMessage;
    private String enableMockTweets;
    private int mockMinTweetLength;
    private int mockMaxTweetLength;
    private long mockSleepMs;
}
