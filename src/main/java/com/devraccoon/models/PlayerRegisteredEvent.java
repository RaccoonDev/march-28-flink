package com.devraccoon.models;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class PlayerRegisteredEvent implements PlayerEvent {
    final Instant eventTime;
    final UUID playerId;

    public PlayerRegisteredEvent(
            Instant eventTime,
            UUID playerId) {
        this.eventTime = eventTime;
        this.playerId = playerId;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public UUID getPlayerId() {
        return playerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlayerRegisteredEvent that = (PlayerRegisteredEvent) o;
        return Objects.equals(eventTime, that.eventTime) && Objects.equals(playerId, that.playerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime, playerId);
    }

    @Override
    public String toString() {
        return "PlayerEvent{" +
                "eventTime=" + eventTime +
                ", playerId=" + playerId +
                '}';
    }
}

