package com.mayreh.kafka.demo;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class Config {
    String bootstrapServers;

    String topic;
}
