package com.devraccoon.models;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class PlayerEvent {
    final Instant eventTime;
    final PlayerEventType eventType;
    final UUID playerId;

    public PlayerEvent(Instant eventTime, PlayerEventType eventType, UUID playerId) {
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.playerId = playerId;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public PlayerEventType getEventType() {
        return eventType;
    }

    public UUID getPlayerId() {
        return playerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlayerEvent that = (PlayerEvent) o;
        return Objects.equals(eventTime, that.eventTime) && eventType == that.eventType && Objects.equals(playerId, that.playerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime, eventType, playerId);
    }

    @Override
    public String toString() {
        return "PlayerEvent{" +
                "eventTime=" + eventTime +
                ", eventType=" + eventType +
                ", playerId=" + playerId +
                '}';
    }
}

