package com.lab.price;

import com.lab.JDBCUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

import java.sql.SQLException;

/**
 *  价格区间
 * @author twl
 */
public class PriceDAO {

    private static QueryRunner runner=new QueryRunner(JDBCUtil.getDataSource());

    /**
     * 插入数据
     * @param price
     * @param count
     */
    public static void insert(String price, Integer count) {
        try {
            runner.update("insert into price(price,count) values(?,?)",price,count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void insertOrUpdate(String price, Integer count) {
        try {
            Price query = runner.query("select * from price where price=?", new BeanHandler<Price>(Price.class), price);
            if (query == null) {
                insert(price,count);
            } else {
                update(price,count);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void update(String price, Integer count) {
        try {
            runner.update("update price set count = ? where price = ?",count,price);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
