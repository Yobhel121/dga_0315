package com.atguigu.dga_0315.common.aspect;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Aspect
@Component
@Slf4j
public class LogAspect {

    Map<String,FuncInfo> funcMap=new HashMap(128);

    @Pointcut("execution(* com.atguigu.dga_0315.ds.service..*.*(..)) " +
            "||execution(* com.atguigu.dga_0315.ds.mapper..*.*(..))"+
            "||execution(* com.atguigu.dga_0315.governance.service..*.*(..))"+
            "||execution(* com.atguigu.dga_0315.governance.assessor..*.*(..))"+
            "||execution(* com.atguigu.dga_0315.governance.mapper..*.*(..))"+
            "||execution(* com.atguigu.dga_0315.meta.service..*.*(..))"+
            "||execution(* com.atguigu.dga_0315.meta.mapper..*.*(..))"
    )
    public void pointCut(){
    }

    @Pointcut("execution(* com.atguigu.dga_0315.governance.service..*.report(..))) ")
     public void pointReport(){
            log.info("!1");
    }


    @Around("pointCut()")
    public Object aroundPoint(ProceedingJoinPoint pjp) throws Throwable {
        long start = System.currentTimeMillis();
        Object result = pjp.proceed();
        long end = System.currentTimeMillis();

       // log.info(pjp.getTarget().getClass().getSimpleName() + "->" + pjp.getSignature().getName() + " 耗费时间:" + (end - start) + "毫秒");
        long durMs = end - start;
        String funcName = pjp.getTarget().getClass().getSimpleName() + "-" + pjp.getSignature().getName();
        if(result!=null){
            funcName+="-"+result.getClass().getSimpleName();
            if(result instanceof ArrayList){
                ArrayList list = (ArrayList) result;
                if(list!=null&&list.size()>0){
                    Object o = list.get(0);
                    funcName+="-" +o.getClass().getSimpleName();
                }

            }

        }

        FuncInfo funcInfo = funcMap.get(funcName);
        if(funcInfo==null){
            funcInfo=new FuncInfo();
        }
        funcInfo.setCount(funcInfo.getCount()+1L);
        funcInfo.setDurMs(funcInfo.getDurMs()+durMs);
        funcMap.put(funcName,funcInfo);
        return  result;
    }


//    @Around("pointReport()")
//    public Object aroundReport(ProceedingJoinPoint pjp) throws Throwable {
//        Object result = pjp.proceed();
//        for (Map.Entry<String, FuncInfo> funcInfoEntry : funcMap.entrySet()) {
//            String funcName=funcInfoEntry.getKey();
//            FuncInfo funcInfo = funcInfoEntry.getValue();
//            Long durMSec = funcInfo.getDurMs();
//            Long count =funcInfo.getCount();
//            Long avgDurMs =durMSec/count;
//
//            log.info("方法:{}   总耗时: {}  次数:{}  平均耗时:{}", funcName,count,avgDurMs);
//        }
//        return  result;
//    }

    @After("pointReport()")
    public void after(JoinPoint joinPoint){

        for (Map.Entry<String, FuncInfo> funcInfoEntry : funcMap.entrySet()) {
            String funcName=funcInfoEntry.getKey();
            FuncInfo funcInfo = funcInfoEntry.getValue();
            Long durMSec = funcInfo.getDurMs();
            Long count =funcInfo.getCount();
            Long avgDurMs =durMSec/count;

            log.info("方法:{}   总耗时: {}  次数:{}  平均耗时:{}", funcName,durMSec,count,avgDurMs);
        }


    }


    @AfterReturning("pointReport()")
    public void afterReturn(){
        log.info("return");
    }

    @Before("pointReport()")
    public void before(){
        log.info("before");
    }



    @Data
    class FuncInfo{
        Long durMs=0L;
        Long count=0L;

    }


    public static void main(String[] args) {

        //下边这组大括号非常重要

        List<String> list = new ArrayList<String>() {};

      //  Map<String, UserInfo> hashMap=new HashMap<>();

//        System.out.println(getActualType(list,0));
//        System.out.println(getActualType(hashMap,0));

    }

    public   String getActualType(Object o,int index) {

        Type clazz = o.getClass().getGenericSuperclass();

        ParameterizedType pt = (ParameterizedType)clazz;

        return StringUtils.substringAfterLast( pt.getActualTypeArguments()[index].getTypeName(),".");

    }


}
