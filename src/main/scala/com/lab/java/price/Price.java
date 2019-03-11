package com.lab.java.price;

import java.io.Serializable;

/**
 * @author twl
 */
public class Price implements Serializable {
    private Integer id;
    private String price;
    private Integer count;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Price{" +
                "id=" + id +
                ", price='" + price + '\'' +
                ", count=" + count +
                '}';
    }
}
