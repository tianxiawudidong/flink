package com.ifchange.flink.async.io;

public class Order {

    private String userId;

    private long orderTime;

    private String orderId;

    private String price;


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(long orderTime) {
        this.orderTime = orderTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }
}
