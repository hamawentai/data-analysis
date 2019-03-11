package com.lab.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 价格区间判断
 * @author twl
 */
public class PriceUtil implements Serializable {

    private static final List<String> PRICE_LIST = new ArrayList<>();

    static {
        PRICE_LIST.add("0-20");
        PRICE_LIST.add("20-50");
        PRICE_LIST.add("50-100");
        PRICE_LIST.add("100-150");
        PRICE_LIST.add("150-200");
        PRICE_LIST.add("200-300");
        PRICE_LIST.add("300-400");
        PRICE_LIST.add("400-500");
        PRICE_LIST.add("500-600");
        PRICE_LIST.add("600-700");
        PRICE_LIST.add("700-800");
        PRICE_LIST.add("800-1000");
        PRICE_LIST.add("1000-1500");
        PRICE_LIST.add("1500-2000");
        PRICE_LIST.add("2000-3000");
        PRICE_LIST.add("3000-4000");
        PRICE_LIST.add("4000-5000");
        PRICE_LIST.add("5000-6000");
        PRICE_LIST.add("6000-7000");
        PRICE_LIST.add("7000-8000");
        PRICE_LIST.add("8000-10000");
        PRICE_LIST.add("10000-15000");
        PRICE_LIST.add("15000-1000000");
    }

    public static Integer getIndex(double price) {

        int index = 0;
        for (String str : PRICE_LIST) {
            String[] splitPrice = str.split("-");
            Double bgPrice = Double.parseDouble(splitPrice[0]);
            Double edPrice = Double.parseDouble(splitPrice[1]);
            if (price>=bgPrice && price<edPrice) {
                break;
            }
            index++;
        }
        return index;
    }

    public static String getPrice(Integer index) {
        String price = PRICE_LIST.get(index);
        return price;
    }

    public static void main(String[] args) {
        String index = PriceUtil.getPrice(0);
        System.out.println(index);
    }
}
