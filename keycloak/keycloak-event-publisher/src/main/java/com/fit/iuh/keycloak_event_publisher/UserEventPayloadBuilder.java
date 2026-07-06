package com.fit.iuh.keycloak_event_publisher;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.keycloak.events.Event;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserEventPayloadBuilder {

    private static final Logger log = Logger.getLogger(UserEventPayloadBuilder.class.getName());

    private final KeycloakSession session;
    private final ObjectMapper mapper;

    public UserEventPayloadBuilder(KeycloakSession session, ObjectMapper mapper) {
        this.session = session;
        this.mapper = mapper;
    }

    public Map<String, Object> fromUserEvent(Event event, String action) {
        Map<String, Object> payload = basePayload(event.getUserId(), action, "USER");
        addCurrentUserProfile(payload, event);
        return payload;
    }

    public Map<String, Object> fromAdminEvent(String userId, String action, String representation) {
        Map<String, Object> payload = basePayload(userId, action, "ADMIN");
        addRepresentationProfile(payload, representation);
        return payload;
    }

    private Map<String, Object> basePayload(String userId, String action, String source) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", userId);
        payload.put("action", action);
        payload.put("source", source);

        return payload;
    }

    private void addCurrentUserProfile(Map<String, Object> payload, Event event) {
        if (event.getRealmId() == null || event.getUserId() == null) {
            return;
        }

        try {
            RealmModel realm = session.realms().getRealm(event.getRealmId());
            if (realm == null) {
                return;
            }

            UserModel user = session.users().getUserById(realm, event.getUserId());
            if (user == null) {
                return;
            }

            payload.put("username", user.getUsername());
            payload.put("email", user.getEmail());
            payload.put("firstName", user.getFirstName());
            payload.put("lastName", user.getLastName());
            payload.put("enabled", user.isEnabled());
            payload.put("emailVerified", user.isEmailVerified());
        } catch (Exception e) {
            log.warning("Could not load user profile from Keycloak session: " + e.getMessage());
        }
    }

    private void addRepresentationProfile(Map<String, Object> payload, String representation) {
        if (representation == null || representation.isEmpty()) {
            return;
        }

        try {
            JsonNode node = mapper.readTree(representation);

            payload.put("username", getText(node, "username"));
            payload.put("email", getText(node, "email"));
            payload.put("firstName", getText(node, "firstName"));
            payload.put("lastName", getText(node, "lastName"));

            JsonNode enabled = node.findValue("enabled");
            if (enabled != null && !enabled.isNull()) {
                payload.put("enabled", enabled.asBoolean());
            }

            JsonNode emailVerified = node.findValue("emailVerified");
            if (emailVerified != null && !emailVerified.isNull()) {
                payload.put("emailVerified", emailVerified.asBoolean());
            }
        } catch (Exception e) {
            log.warning("Could not parse admin event representation: " + e.getMessage());
        }
    }

    private String getText(JsonNode node, String fieldName) {
        JsonNode value = node.findValue(fieldName);
        if (value == null || value.isNull()) {
            return null;
        }

        return value.asText();
    }
}
