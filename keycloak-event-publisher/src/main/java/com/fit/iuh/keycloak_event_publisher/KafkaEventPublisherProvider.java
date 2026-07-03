package com.fit.iuh.keycloak_event_publisher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.logging.Logger;

public record KafkaEventPublisherProvider(KafkaProducer<String, String> producer, String topic) implements EventListenerProvider {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(KafkaEventPublisherProvider.class.getName());

    @Override
    public void onEvent(Event event) {
        String email = event.getDetails() == null ? null : event.getDetails().get("email");

        switch (event.getType()) {
            case REGISTER -> sendToKafka(event.getUserId(), email, "CREATE", "USER");
            default -> {}
        }
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
        if (adminEvent.getResourceType() != null && adminEvent.getResourceType().name().startsWith("USER")) {
            String userId = extractUserId(adminEvent.getResourcePath());
            switch (adminEvent.getOperationType()) {

                case CREATE -> {
                    String defaultMail = "admin-" + System.currentTimeMillis();
                    String extractedEmail = null;

                    if (includeRepresentation && adminEvent.getRepresentation() != null)
                        extractedEmail = extractEmailFromRep(adminEvent.getRepresentation());

                    String finalEmail = (extractedEmail != null) ? extractedEmail : defaultMail;
                    sendToKafka(userId, finalEmail, "CREATE", "ADMIN");
                }

                default -> {}
            }
        }

    }

    @Override
    public void close() {
        producer.flush();
    }

    private void sendToKafka(String userId, String email, String action, String source) {
        try {
            java.util.Map<String, String> data = new java.util.HashMap<>();
            data.put("userId", userId);
            data.put("email", email != null ? email : "null");
            data.put("action", action);
            data.put("source", source);

            String payload = mapper.writeValueAsString(data);

            producer.send(new ProducerRecord<>(topic, userId, payload));
        } catch (Exception e) {
            log.severe("Kafka Error (Jackson): " + e.getMessage());
        }
    }

    private String extractUserId(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }

        return path.startsWith("users/") ? path.substring(6) : path;
    }

    private String extractEmailFromRep(String representation) {
        if (representation == null || representation.isEmpty()) {
            return null;
        }
        try {
            JsonNode node = mapper.readTree(representation);

            if (node.has("email")) return node.get("email").asText();
        } catch (Exception e) {
            log.warning("Could not parse admin event representation: " + e.getMessage());
        }

        return null;
    }
    
}
