package com.devraccoon.models;

import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class TaxiRide implements Serializable {
    public long rideId;
    public boolean isStart;
    public short passengerCnt;
    public long taxiId;
    public long driverId;
    public long eventTimeMillis;

    public TaxiRide() {
    }

    public TaxiRide(long rideId, boolean isStart,  short passengerCnt, long taxiId, long driverId, long eventTimeMillis) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
        this.eventTimeMillis = eventTimeMillis;
    }

    public void setRideId(long rideId) {
        this.rideId = rideId;
    }

    public void setStart(boolean start) {
        isStart = start;
    }


    public void setPassengerCnt(short passengerCnt) {
        this.passengerCnt = passengerCnt;
    }

    public void setTaxiId(long taxiId) {
        this.taxiId = taxiId;
    }

    public void setDriverId(long driverId) {
        this.driverId = driverId;
    }

    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }

    public long getRideId() {
        return rideId;
    }

    public boolean isStart() {
        return isStart;
    }

    public Instant getEventTime() {
        return Instant.ofEpochMilli(eventTimeMillis);
    }

    public short getPassengerCnt() {
        return passengerCnt;
    }

    public long getTaxiId() {
        return taxiId;
    }

    public long getDriverId() {
        return driverId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) {return false;}
        TaxiRide taxiRide = (TaxiRide) o;
        return isStart == taxiRide.isStart &&
                passengerCnt == taxiRide.passengerCnt &&
                taxiId == taxiRide.taxiId &&
                driverId == taxiRide.driverId &&
                rideId == taxiRide.rideId ;
    }


    /** Gets the ride's time stamp as a long in millis since the epoch. */
    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rideId, isStart, passengerCnt, taxiId, driverId);
    }
}
