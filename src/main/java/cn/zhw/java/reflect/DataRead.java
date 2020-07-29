package cn.zhw.java.reflect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class DataRead {
    public static <T> ArrayList<T> read (String path, Class<T> clazz){
        ArrayList<T> list = new ArrayList<>();

        try {
            BufferedReader bufferedReader = new BufferedReader(
                    new FileReader(path));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split(",");

                // 创建对象
                T t = clazz.newInstance();

                /*
                 * 给对象所有的属性赋值
                 */

                // 获取类所有的属性
                Field[] fields = clazz.getDeclaredFields();

                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];

                    // 获取属性类型
                    Class<?> type = field.getType();

                    // 获取属性名称
                    String name = field.getName();
                    // tpUpperCase() 方法转大写
                    String monthName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);

                    // 构建 set 方法给属性赋值
                    if (type == Integer.class) {
                        //获取方法对象
                        Method method = clazz.getMethod(monthName, Integer.class);

                        //执行方法
                        method.invoke(t, Integer.parseInt(split[i]));
                    } else {
                        //获取方法对象
                        Method method = clazz.getMethod(monthName, String.class);

                        //执行方法
                        method.invoke(t, split[i]);
                    }
                }
                list.add(t);
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
}
