package com.devraccoon.models;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public interface PlayerEvent {
    Instant getEventTime();
    UUID getPlayerId();
}
