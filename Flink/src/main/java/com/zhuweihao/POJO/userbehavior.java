package com.zhuweihao.POJO;

/**
 * @Author zhuweihao
 * @Date 2022/12/8 14:10
 * @Description com.zhuweihao.POJO
 */
public class userbehavior {
    private int user_id;
    private int item_id;
    private int category_id;
    private String behavior_type;
    private int timestamp;

    @Override
    public String toString() {
        return "userbehavior{" +
                "user_id=" + user_id +
                ", item_id=" + item_id +
                ", category_id=" + category_id +
                ", behavior_type=" + behavior_type +
                ", timestamp=" + timestamp +
                '}';
    }

    public userbehavior() {
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public int getItem_id() {
        return item_id;
    }

    public void setItem_id(int item_id) {
        this.item_id = item_id;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public String getBehavior_type() {
        return behavior_type;
    }

    public void setBehavior_type(String behavior_type) {
        this.behavior_type = behavior_type;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
}
