package com.lab.java;

import java.io.Serializable;

public class HotWord implements Serializable {

    private Long id;

    private String word;

    private Integer number;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    @Override
    public String toString() {
        return "HotWord{" +
                "id=" + id +
                ", word='" + word + '\'' +
                ", number=" + number +
                '}';
    }
}
