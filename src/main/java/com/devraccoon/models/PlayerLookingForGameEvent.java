package com.devraccoon.models;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class PlayerLookingForGameEvent implements PlayerEvent {
    final Instant eventTime;
    final UUID playerId;
    final GameType gameType;

    public PlayerLookingForGameEvent(
            Instant eventTime,
            UUID playerId, GameType gameType) {
        this.eventTime = eventTime;
        this.playerId = playerId;
        this.gameType = gameType;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public UUID getPlayerId() {
        return playerId;
    }

    public GameType getGameType() {
        return gameType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlayerLookingForGameEvent that = (PlayerLookingForGameEvent) o;
        return Objects.equals(eventTime, that.eventTime) && Objects.equals(playerId, that.playerId) && gameType == that.gameType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime, playerId, gameType);
    }

    @Override
    public String
    toString() {
        return "PlayerLookingForGameEvent{" +
                "eventTime=" + eventTime +
                ", playerId=" + playerId +
                ", gameType=" + gameType +
                '}';
    }
}
