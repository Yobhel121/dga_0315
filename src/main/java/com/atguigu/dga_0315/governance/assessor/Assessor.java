package com.atguigu.dga_0315.governance.assessor;

import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;

public abstract class  Assessor {
    public final GovernanceAssessDetail metricAssess(AssessParam assessParam) {
        GovernanceAssessDetail governanceAssessDetail = new GovernanceAssessDetail();
        //1 获得元数据信息
        //2 获得指标信息
        //3  填写考评信息 （填写默认值的逻辑每个指标都一样)
        governanceAssessDetail.setAssessDate(assessParam.getAssessDate());
        governanceAssessDetail.setSchemaName(assessParam.getTableMetaInfo().getSchemaName());
        governanceAssessDetail.setTableName(assessParam.getTableMetaInfo().getTableName());
        governanceAssessDetail.setMetricId(assessParam.getGovernanceMetric().getId().toString());
        governanceAssessDetail.setMetricName(assessParam.getGovernanceMetric().getMetricName());
        governanceAssessDetail.setGovernanceType(assessParam.getGovernanceMetric().getGovernanceType());
        governanceAssessDetail.setTecOwner(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName());
        governanceAssessDetail.setAssessScore(BigDecimal.TEN);
        //4  判断得分逻辑  问题信息  备注  url  每个指标不同
        try {
            checkProblem(governanceAssessDetail, assessParam);
        }catch (Exception e){
            governanceAssessDetail.setIsAssessException("1");
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);

            int maxLimit =stringWriter.toString().length()>2000?2000:stringWriter.toString().length();

            governanceAssessDetail.setAssessExceptionMsg(stringWriter.toString().substring(0,maxLimit));
        }
        //5  处理考评是的异常   每个指标都一样
        //6   考评时间  每个指标都一样
        governanceAssessDetail.setCreateTime(new Date());
        return governanceAssessDetail;

    }

    public abstract void  checkProblem(GovernanceAssessDetail governanceAssessDetail,AssessParam assessParam) throws Exception;

}
