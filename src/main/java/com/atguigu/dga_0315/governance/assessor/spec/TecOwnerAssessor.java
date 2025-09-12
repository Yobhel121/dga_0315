package com.atguigu.dga_0315.governance.assessor.spec;

import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("HAS_TEC_OWNER")
public class TecOwnerAssessor extends Assessor {


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String tecOwnerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName();
        if(tecOwnerUserName==null||tecOwnerUserName.trim().length()==0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("未填写技术owner");
            String governanceUrl = assessParam.getGovernanceMetric().getGovernanceUrl().replace("{tableId}", assessParam.getTableMetaInfo().getId().toString());
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }

    }
}
