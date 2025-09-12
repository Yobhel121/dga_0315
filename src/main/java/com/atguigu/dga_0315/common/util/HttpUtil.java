package com.atguigu.dga_0315.common.util;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

public class HttpUtil {


    static OkHttpClient okHttpClient= new OkHttpClient();


    public  static String get(String url){
         //创建一个request
        Request request = new Request.Builder().get().url(url).build();
        //创建会话
        Call call = okHttpClient.newCall(request);

        try {
            Response response = call.execute();
            return   response.body().string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        String json = HttpUtil.get("http://hadoop102:18080/api/v1/applications/application_1682294710257_0090/1/stages/1");
        System.out.println("json = " + json);
    }
}
