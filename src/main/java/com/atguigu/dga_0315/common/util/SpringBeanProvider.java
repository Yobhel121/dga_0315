package com.atguigu.dga_0315.common.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;


// 人力资源部
//  1    实现 ApplicationContextAware  // 人力资源管理接口
//  2    setApplicationContext //接收总部下发员工花名册
//  3    根据需要随时 通过getBean方法获得 指名指类的对象实例
@Component
public class SpringBeanProvider implements ApplicationContextAware {

    ApplicationContext applicationContext=null;



    public <T>  T  getBean(String beanName ,Class<T> tClass) {

        return applicationContext.getBean(beanName,tClass);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext=applicationContext;
    }
}
