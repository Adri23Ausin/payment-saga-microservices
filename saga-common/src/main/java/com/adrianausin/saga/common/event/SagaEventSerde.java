package com.adrianausin.saga.common.event;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.json.JsonMapper;

/** JSON for saga events, used for Kafka values and for the outbox. */
public final class SagaEventSerde {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private SagaEventSerde() {
    }

    public static String toJson(SagaEvent event) {
        return MAPPER.writeValueAsString(event);
    }

    public static SagaEvent fromJson(String json) {
        return MAPPER.readValue(json, SagaEvent.class);
    }

    public static class EventSerializer implements Serializer<SagaEvent> {

        @Override
        public byte[] serialize(String topic, SagaEvent event) {
            return event == null ? null : MAPPER.writeValueAsBytes(event);
        }
    }

    public static class EventDeserializer implements Deserializer<SagaEvent> {

        @Override
        public SagaEvent deserialize(String topic, byte[] bytes) {
            return bytes == null ? null : MAPPER.readValue(bytes, SagaEvent.class);
        }
    }
}
