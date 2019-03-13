package com.lab;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

import java.sql.SQLException;

public class HotWordDAO {

    private static QueryRunner runner=new QueryRunner(JDBCUtil.getDataSource());

    public static void getNumber(String word, Integer number) {
        try {
            HotWord query = runner.query("select * from hot_word where word=?", new BeanHandler<HotWord>(HotWord.class), word);
            if (query == null) {
                insert(word,number);
            } else {
                update(word,number+query.getNumber());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void update(String word, Integer number) {
        try {
            runner.update("update hot_word set number=? where word=?",number,word);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void insert(String word, Integer number) {
        try {
            runner.update("insert into hot_word(word,number) values(?,?)",word,number);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HotWordDAO.getNumber("可乐",2);
    }
}
