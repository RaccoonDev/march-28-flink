package com.devraccoon.models;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class GameFound {
    private final Set<UUID> playerIds;
    private final GameType gameType;

    public GameFound(Set<UUID> playerIds, GameType gameType) {
        this.playerIds = playerIds;
        this.gameType = gameType;
    }

    public Set<UUID> getPlayerIds() {
        return playerIds;
    }

    public GameType getGameType() {
        return gameType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GameFound gameFound = (GameFound) o;
        return Objects.equals(playerIds, gameFound.playerIds) && gameType == gameFound.gameType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(playerIds, gameType);
    }

    @Override
    public String toString() {
        return "GameFound{" +
                "playerIds=" + playerIds +
                ", gameType=" + gameType +
                '}';
    }
}
