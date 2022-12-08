package com.zhuweihao;

import com.zhuweihao.POJO.supplier;
import com.zhuweihao.Utils.JSONUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author zhuweihao
 * @Date 2022/12/7 18:44
 * @Description com.zhuweihao
 */
public class test {
    public static void main(String[] args) {
        long lo=1511544070L;
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //long转Date
        Date date = new Date(lo);
        System.out.println(sd.format(date));
    }
}
