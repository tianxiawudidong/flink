package com.ifchange.flink.table;

import java.io.Serializable;

public class MyIcdc implements Serializable {

    //String, String, Double, Integer
    private String wcm;

    private String hostName;

    private double responseTime;

    private int success;

    public String getWcm() {
        return wcm;
    }

    public void setWcm(String wcm) {
        this.wcm = wcm;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public double getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(double responseTime) {
        this.responseTime = responseTime;
    }

    public int getSuccess() {
        return success;
    }

    public void setSuccess(int success) {
        this.success = success;
    }
}
