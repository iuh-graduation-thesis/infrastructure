package com.fit.iuh.keycloak_event_publisher;

import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

public record KafkaEventPublisherProvider(KafkaEventPublisher publisher, UserEventPayloadBuilder payloadBuilder)
        implements EventListenerProvider {

    @Override
    public void onEvent(Event event) {
        switch (event.getType().name()) {
            case "REGISTER" -> publisher.publish(event.getUserId(), payloadBuilder.fromUserEvent(event, "CREATE"));
            case "UPDATE_PROFILE", "UPDATE_EMAIL" -> publisher.publish(event.getUserId(), payloadBuilder.fromUserEvent(event, "UPDATE"));
            case "DELETE_ACCOUNT" -> publisher.publish(event.getUserId(), payloadBuilder.fromUserEvent(event, "DELETE"));
            default -> {}
        }
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
        if (adminEvent.getResourceType() != null && adminEvent.getResourceType().name().startsWith("USER")) {
            String userId = extractUserId(adminEvent.getResourcePath());
            switch (adminEvent.getOperationType()) {

                case CREATE -> publisher.publish(userId, payloadBuilder.fromAdminEvent(userId, "CREATE", adminEvent.getRepresentation()));
                case UPDATE -> publisher.publish(userId, payloadBuilder.fromAdminEvent(userId, "UPDATE", adminEvent.getRepresentation()));
                case DELETE -> publisher.publish(userId, payloadBuilder.fromAdminEvent(userId, "DELETE", adminEvent.getRepresentation()));
                default -> {}
            }
        }

    }

    @Override
    public void close() {
        publisher.flush();
    }

    private String extractUserId(String resourcePath) {
        if (resourcePath == null || resourcePath.isBlank()) {
            return null;
        }

        return resourcePath.startsWith("users/") ? resourcePath.substring(6) : resourcePath;
    }
}
