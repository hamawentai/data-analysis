package com.lab.java.classify;

import com.lab.java.JDBCUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

import java.sql.SQLException;

public class ClassifyDAO {

    private static QueryRunner runner=new QueryRunner(JDBCUtil.getDataSource());

    public static void main(String[] args) {
        ClassifyDAO.insertSecond("手机数码","手机配件");
       // ClassifyDAO.insertThree("dd","ee");
    }

    //添加二级分类
    public static void insertSecond(String label,String name) {
        Integer idBySecond = findIdBySecond(name);
        if (idBySecond != -1) {
            return;
        }

        Integer labelId = findIdByLabel(label);
        try {
            runner.update("insert into classify_second(name,label_id) values(?,?)",name,labelId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //添加三级分类
    public static void insertThree(String secondName,String threeName) {

        Integer threeByName = findThreeByName(threeName);
        if (threeByName != -1) {
            return;
        }

        Integer secondId = findIdBySecond(secondName);
        try {
            runner.update("insert into classify_three(name,second_id) values(?,?)",threeName,secondId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Integer findThreeByName(String name) {
        try {
            ClassifyThree classifyThree = runner.query("select * from classify_second where name=?", new BeanHandler<ClassifyThree>(ClassifyThree.class), name);
            if (classifyThree != null) {
                return 1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    //根据一级分类名 查询一级分类id
    private static Integer findIdByLabel(String label) {
        Label label1 = null;
        try {
            label1 = runner.query("select * from label where label=?", new BeanHandler<Label>(Label.class), label);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(label);
        return label1.getId();
    }

    //根据二级分类 查询二级分类id
    private static Integer findIdBySecond(String name) {
        ClassifySecond classifySecond = null;
        try {
            classifySecond = runner.query("select * from classify_second where name=?", new BeanHandler<ClassifySecond>(ClassifySecond.class), name);
            if (classifySecond != null) {
                return classifySecond.getId();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;
    }
}
