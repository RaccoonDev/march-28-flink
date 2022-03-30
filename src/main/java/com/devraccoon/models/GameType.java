package com.devraccoon.models;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.StreamSupport;

public enum GameType {
    ONE_VS_ONE(2),
    TWO_VS_TWO(4),
    THREE_VS_THREE(6),
    FOUR_VS_FOUR(8);

    private final int totalNumberOfPlayers;

    GameType(int totalNumberOfPlayers) {
        this.totalNumberOfPlayers = totalNumberOfPlayers;
    }

    public static Optional<GameType> forValue(int currentKey) {
        return Arrays.stream(values()).filter(e -> e.totalNumberOfPlayers == currentKey).findFirst();
    }

    public int getTotalNumberOfPlayers() {
        return totalNumberOfPlayers;
    }
}
