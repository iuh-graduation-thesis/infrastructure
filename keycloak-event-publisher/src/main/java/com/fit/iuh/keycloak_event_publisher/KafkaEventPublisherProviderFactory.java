package com.fit.iuh.keycloak_event_publisher;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;

public class KafkaEventPublisherProviderFactory implements EventListenerProviderFactory {

    private static final String PROVIDER_ID = "kafka-event-publisher";

    private static final String BOOTSTRAP_SERVERS_KEY = "bootstrapServers";
    private static final String TOPIC_KEY = "topic";

    private static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVER";
    private static final String TOPIC_ENV = "KEYCLOAK_KAFKA_TOPIC";

    private static final String BOOTSTRAP_SERVERS_DEFAULT = "kafka:19092";
    private static final String TOPIC_DEFAULT = "user-registration";

    private KafkaProducer<String, String> producer;
    private String topic;

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ProviderConfigurationBuilder.create()
                .property()
                .name(BOOTSTRAP_SERVERS_KEY)
                .label("Kafka Bootstrap Servers")
                .helpText("Kafka bootstrap servers, for example kafka:19092.")
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(BOOTSTRAP_SERVERS_DEFAULT)
                .add()
                .property()
                .name(TOPIC_KEY)
                .label("Kafka Topic")
                .helpText("Kafka topic used to publish Keycloak user events.")
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(TOPIC_DEFAULT)
                .add()
                .build();
    }

    @Override
    public void init(Config.Scope config) {
        String bootstrapServers = resolveConfig(config, BOOTSTRAP_SERVERS_KEY, BOOTSTRAP_SERVERS_ENV, BOOTSTRAP_SERVERS_DEFAULT);
        topic = resolveConfig(config, TOPIC_KEY, TOPIC_ENV, TOPIC_DEFAULT);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, PROVIDER_ID);

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new KafkaEventPublisherProvider(producer, topic);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    private static String resolveConfig(Config.Scope config, String key, String envName, String defaultValue) {
        String value = config.get(key);
        if (hasText(value)) {
            return value;
        }

        value = System.getenv(envName);
        if (hasText(value)) {
            return value;
        }

        return defaultValue;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
