package com.atguigu.dga_0315.governance.assessor.calc;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component("NO_ACCESS")
public class NoAccessAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {
        //  1  获取 days 参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObj = JSON.parseObject(metricParamsJson);
        Long paramDays = paramJsonObj.getLong("days");

        //  2  获取当前表的最后访问日期
        Date tableLastAccessTime = assessParam.getTableMetaInfo().getTableLastAccessTime();
        //  3  获取 考评日期
        String assessDate=assessParam.getAssessDate();
        //  4  求考评日期 到最后访问日期的 日期天数差值  （只看日期 ，不考虑时间)
        //  4.1  把最后访问日期 抹去时间部分
        Date tableLastAccessDate = DateUtils.truncate(tableLastAccessTime, Calendar.DATE);

        //  4.2 把 考评日期 转为 时间格式
        Date assessDateDt = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        //4.3  求差
        long diffMs = assessDateDt.getTime() - tableLastAccessDate.getTime();
        long diffDays = TimeUnit.DAYS.convert(diffMs, TimeUnit.MILLISECONDS);
        //  5  天数差值和 days参数作比较  超过则给0分 差评原因
        if(diffDays> paramDays ) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("最后访问日期超过"+paramDays+"天,最后访问日期为"+ DateFormatUtils.format(tableLastAccessDate,"yyyy-MM-dd") );
        }

    }
}
