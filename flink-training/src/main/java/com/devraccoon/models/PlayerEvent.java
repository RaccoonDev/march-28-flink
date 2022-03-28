package com.devraccoon.models;

import java.time.Instant;
import java.util.Objects;

public class PlayerEvent {
    final Instant eventTime;
    final PlayerEventType eventType;

    public PlayerEvent(Instant eventTime, PlayerEventType eventType) {
        this.eventTime = eventTime;
        this.eventType = eventType;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public PlayerEventType getEventType() {
        return eventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlayerEvent that = (PlayerEvent) o;
        return Objects.equals(eventTime, that.eventTime) && eventType == that.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime, eventType);
    }

    @Override
    public String toString() {
        return "PlayerEvent{" +
                "eventTime=" + eventTime +
                ", eventType=" + eventType +
                '}';
    }
}

