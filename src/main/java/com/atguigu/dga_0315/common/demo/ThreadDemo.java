package com.atguigu.dga_0315.common.demo;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ThreadDemo {


    public static void main(String[] args) throws InterruptedException {
        // 计算每个数的平方数 ，再累加起来
        Integer[]  nums={1,2,3,4,5,6};
        long startMs = System.currentTimeMillis();
        List<CompletableFuture<Integer>> futureList=new ArrayList<>();
        List<Integer> num2List=new ArrayList<>();
        for (Integer num : nums) {   //计算平方
            //异步编排
            CompletableFuture<Integer> future = CompletableFuture.supplyAsync( ()-> {
                Integer num2 = num * num;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return num2;
            });
            futureList.add(future);
            // num2List.add(num2);  //现货列表
        }
        // 期货的兑现
        List<Integer> num22List = futureList.stream().map(future -> future.join()).collect(Collectors.toList());
        Integer sum=0;
        for (Integer num : num22List) {
            sum+=num;
        }

        System.out.println(" 耗时：  " +(System.currentTimeMillis()-startMs)) ;
        System.out.println("sum = " + sum);  //汇总

    }
}
