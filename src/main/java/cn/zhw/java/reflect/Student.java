package cn.zhw.java.reflect;

public class Student {
    private String id;
    private String  name;
    private Integer age;
    private String gender;
    private String clazz;

    public Student() {
    }

    public Student(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                ", clazz='" + clazz + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public Student(String id, String name, Integer age, String gender, String clazz) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.gender = gender;
        this.clazz = clazz;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

}
