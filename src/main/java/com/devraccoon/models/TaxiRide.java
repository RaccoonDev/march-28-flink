package com.devraccoon.models;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class TaxiRide implements Serializable {
    public long rideId;
    public boolean isStart;
    public long passengerCnt;
    public long taxiId;
    public long driverId;
    public long eventTime;

    public TaxiRide() {
    }

    public TaxiRide(long rideId, boolean isStart,  long passengerCnt, long taxiId, long driverId, long eventTime) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
        this.eventTime = eventTime;
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

    public void setEventTime(long eventTimeMillis) {
        this.eventTime = eventTimeMillis;
    }

    public long getRideId() {
        return rideId;
    }

    public boolean isStart() {
        return isStart;
    }

    public Instant getEventTime() {
        return Instant.ofEpochMilli(eventTime);
    }

    public long getPassengerCnt() {
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
        return eventTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rideId, isStart, passengerCnt, taxiId, driverId);
    }
}
