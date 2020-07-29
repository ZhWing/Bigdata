package cn.zhw.java.reflect;

public class Score {
    private String sid;
    private String cid;
    private Integer score;

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Score{" +
                "sid='" + sid + '\'' +
                ", cid='" + cid + '\'' +
                ", score=" + score +
                '}';
    }

    public Score(String sid, String cid, Integer score) {
        this.sid = sid;
        this.cid = cid;
        this.score = score;
    }

    public Score() {
    }
}
