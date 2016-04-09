package com.kafka.sparkstreaming.model;

import java.io.Serializable;

/**
 * Created by Nagendra on 4/9/16.
 */
public class Meter implements Serializable{

    private String meterID;
    private String meterStatus;
    private String meterTime;

    public String getMeterID() {
        return meterID;
    }

    public void setMeterID(String meterID) {
        this.meterID = meterID;
    }

    public String getMeterStatus() {
        return meterStatus;
    }

    public void setMeterStatus(String meterStatus) {
        this.meterStatus = meterStatus;
    }

    public String getMeterTime() {
        return meterTime;
    }

    public void setMeterTime(String meterTime) {
        this.meterTime = meterTime;
    }
}
