package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MainAssessServiceImpl  implements MainAssessService {

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Autowired
    GovernanceAssessTableService governanceAssessTableService;

    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Override
    public void mainAssess(String assessDate) {
        // 1 指标的考评
        governanceAssessDetailService.allMetricAssess(assessDate);
        // 2 表、人、全局的分数计算
        governanceAssessTableService.genGovenanceAssessTable(assessDate);

        governanceAssessTecOwnerService.genGovenanceAssessTecOwner(assessDate);

        governanceAssessGlobalService.genGovenanceAssessGlobal(assessDate);
    }
}
