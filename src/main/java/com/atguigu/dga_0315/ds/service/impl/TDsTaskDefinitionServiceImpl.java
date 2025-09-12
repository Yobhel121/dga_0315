package com.atguigu.dga_0315.ds.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.ds.bean.TDsTaskDefinition;
import com.atguigu.dga_0315.ds.mapper.TDsTaskDefinitionMapper;
import com.atguigu.dga_0315.ds.service.TDsTaskDefinitionService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-25
 */
@Service
@DS("ds")
public class TDsTaskDefinitionServiceImpl extends ServiceImpl<TDsTaskDefinitionMapper, TDsTaskDefinition> implements TDsTaskDefinitionService {

    @Override
    public List<TDsTaskDefinition> getTaskDefinitionListForAssess(List<Long> taskCodeList) {
        //1 查库
        List<TDsTaskDefinition> tDsTaskDefinitionList = this.list(new QueryWrapper<TDsTaskDefinition>().in("code", taskCodeList));
        //2 提sql
        for (TDsTaskDefinition tDsTaskDefinition : tDsTaskDefinitionList) {
              String sql=   extractSQL( tDsTaskDefinition.getTaskParams());
              tDsTaskDefinition.setSql(sql);
        }
        return tDsTaskDefinitionList;
    }
    //1 json对象化  ，从json对象中提取 rawScript的值  shell脚本
    //2  从shell脚本，切分出sql 语句 。
    private String extractSQL(String taskParams) {
        //1 json对象化  ，从json对象中提取 rawScript的值  shell脚本
        JSONObject taskParamJsonObj = JSON.parseObject(taskParams);
        String shell = taskParamJsonObj.getString("rawScript");
        //2  从shell脚本，切分出sql 语句 。
        // 2.1  开头 有 with 用with 开头 ，没有with 用insert
        int startIdx = shell.indexOf("with");
        if(startIdx ==-1){
            startIdx=shell.indexOf("insert");
        }
        if(startIdx==-1){  //说明 脚本中没有sql
            return null;
        }

        // 2.2  结尾下标：  从startIdx开始 后面出现的第一个分号的下标，如果没有分号 取 第一个双引
        int endIdx = shell.indexOf(";", startIdx);
        if(endIdx== -1){
            endIdx = shell.indexOf("\"", startIdx);
        }

        String sql = shell.substring(startIdx, endIdx);


        return sql;
    }
}
