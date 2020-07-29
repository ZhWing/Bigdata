package cn.zhw.java.reflect;

public class Cource {
    private String id;
    private String name;
    private Integer score;

    @Override
    public String toString() {
        return "Cource{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", score=" + score +
                '}';
    }

    public String getId() {
        return id;
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

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public Cource() {
    }

    public Cource(String id, String name, Integer score) {
        this.id = id;
        this.name = name;
        this.score = score;
    }
}
