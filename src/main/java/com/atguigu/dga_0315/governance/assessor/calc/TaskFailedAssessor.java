package com.atguigu.dga_0315.governance.assessor.calc;

import com.atguigu.dga_0315.common.constant.MetaConst;
import com.atguigu.dga_0315.ds.bean.TDsTaskInstance;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component("TASK_FAILED")
public class TaskFailedAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {


        //1  从参数中提取 任务实例列表
        List<TDsTaskInstance> tDsTaskInstancesList = assessParam.getTDsTaskInstancesList();

        if(tDsTaskInstancesList==null){
            return;
        }

        //2 循环检查是否有失败状态 如果有 给0分 差评
        boolean isFailed=false;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstancesList) {
            if(tDsTaskInstance.getState().toString().equals(MetaConst.TASK_STATE_FAILED)){
                isFailed=true;
            }

        }

        if(isFailed){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在失败任务");
        }

    }
}
