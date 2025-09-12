package com.atguigu.dga_0315.governance.assessor.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.common.constant.MetaConst;
import com.atguigu.dga_0315.ds.bean.TDsTaskInstance;
import com.atguigu.dga_0315.ds.service.TDsTaskInstanceService;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component("TIME_LINESS")
public class TimeLinessAssessor extends Assessor {

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        //1    取当日最后成功的任务实例
        List<TDsTaskInstance> tDsTaskInstancesList = assessParam.getTDsTaskInstancesList();
        if(tDsTaskInstancesList==null){
            return;
        }

        TDsTaskInstance  lastTaskInstance=null;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstancesList) {
            if(tDsTaskInstance.getState().toString().equals(MetaConst.TASK_STATE_SUCCESS)){
                if(lastTaskInstance==null){
                    lastTaskInstance=tDsTaskInstance;
                }else if(lastTaskInstance.getEndTime().compareTo(tDsTaskInstance.getEndTime())<0){
                    lastTaskInstance= tDsTaskInstance;   //更靠后的任务
                }
            }
        }


        //2    取 days参数  percent参数
        String paramJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObj = JSON.parseObject(paramJson);
        Integer days = paramJsonObj.getInteger("days");
        BigDecimal paramPercent = paramJsonObj.getBigDecimal("percent");

        //3    根据days查询数据库 的成功的任务列表  // 参数 ：  1 要查询的日期列表  ,2  成功的, 3 当前考评表的
        //获得前n天日期列表
        List<String> beforeDateList =new ArrayList<>();
        String assessDate = assessParam.getAssessDate();
        Date assessDt = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        for (int i = 1; i <=days; i++) {
            Date beforeDate = DateUtils.addDays(assessDt, 0 - i);
            String beforeDateString = DateFormatUtils.format(beforeDate, "yyyy-MM-dd");
            beforeDateList.add(beforeDateString);
        }
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();

        QueryWrapper<TDsTaskInstance> queryWrapper = new QueryWrapper<TDsTaskInstance>().in("date_format(start_time,'%Y-%m-%d')", beforeDateList)
                .eq("state", MetaConst.TASK_STATE_SUCCESS)
                .eq("name", tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());

        List<TDsTaskInstance> beforeDaysTaskInstanceList = tDsTaskInstanceService.list(queryWrapper);
        if(beforeDaysTaskInstanceList==null){
            return;
        }

        //4    求成功任务耗时的平均值
        long sumDurationMs=0L;
        for (TDsTaskInstance tDsTaskInstance : beforeDaysTaskInstanceList) {
            long durationMs = tDsTaskInstance.getEndTime().getTime() - tDsTaskInstance.getStartTime().getTime();
            sumDurationMs+=durationMs;
        }
        long avgDuration=sumDurationMs/beforeDaysTaskInstanceList.size();


        long lastDurationMs = lastTaskInstance.getEndTime().getTime() - lastTaskInstance.getStartTime().getTime();

        //5   当日耗时- 平均耗时/ 平均耗时   对比percent
        BigDecimal curPercent = BigDecimal.valueOf(lastDurationMs-avgDuration).movePointRight(2).divide(BigDecimal.valueOf(avgDuration),2,BigDecimal.ROUND_HALF_UP);

        //6  如果超过 0分 差评

        if(curPercent.compareTo(paramPercent)>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("当日计算耗时超过前"+days+"天平均耗时"+curPercent);
        }

    }
}
