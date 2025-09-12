package com.atguigu.dga_0315.governance.bean;

import com.atguigu.dga_0315.ds.bean.TDsTaskDefinition;
import com.atguigu.dga_0315.ds.bean.TDsTaskInstance;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AssessParam {

    // 考评日期
    String assessDate;

    // 元数据
    TableMetaInfo tableMetaInfo;

    // 指标
    GovernanceMetric governanceMetric;

    // 全部元数据集合
    List<TableMetaInfo> tableMetaInfoList;

    // 全部元数据的集合 map版
    Map<String,TableMetaInfo> tableMetaInfoMap;

    // ds信息...
    //当前考评表的任务定义
    TDsTaskDefinition tDsTaskDefinition;


    //当前考评表的任务实例
    List<TDsTaskInstance> tDsTaskInstancesList;

}
