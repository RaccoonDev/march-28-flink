package com.devraccoon.models;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class GameFound {
    private final Set<UUID> playerIds;
    private final GameType gameType;
    private final String mapName;

    public GameFound(Set<UUID> playerIds, GameType gameType, String mapName) {
        this.playerIds = playerIds;
        this.gameType = gameType;
        this.mapName = mapName;
    }

    public Set<UUID> getPlayerIds() {
        return playerIds;
    }

    public GameType getGameType() {
        return gameType;
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GameFound gameFound = (GameFound) o;
        return Objects.equals(playerIds, gameFound.playerIds) && gameType == gameFound.gameType && Objects.equals(mapName, gameFound.mapName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(playerIds, gameType, mapName);
    }

    @Override
    public String toString() {
        return "GameFound{" +
                "playerIds=" + playerIds +
                ", gameType=" + gameType +
                ", mapName='" + mapName + '\'' +
                '}';
    }
}
