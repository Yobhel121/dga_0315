package com.atguigu.dga_0315;

import com.atguigu.dga_0315.governance.service.GovernanceAssessGlobalService;
import com.atguigu.dga_0315.governance.service.GovernanceAssessTableService;
import com.atguigu.dga_0315.governance.service.GovernanceAssessTecOwnerService;
import com.atguigu.dga_0315.governance.service.MainAssessService;
import com.atguigu.dga_0315.governance.service.impl.GovernanceAssessDetailServiceImpl;
import com.atguigu.dga_0315.governance.service.impl.MainAssessServiceImpl;
import com.atguigu.dga_0315.lineage.bean.GovernanceLineageTable;
import com.atguigu.dga_0315.lineage.service.GovernanceLineageTableService;
import com.atguigu.dga_0315.lineage.service.impl.GovernanceLineageTableServiceImpl;
import com.atguigu.dga_0315.meta.service.impl.TableMetaInfoServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class Dga0315ApplicationTests {

    @Autowired
    TableMetaInfoServiceImpl tableMetaInfoServiceImpl;

    @Autowired
    GovernanceAssessDetailServiceImpl governanceAssessDetailService;

    @Autowired
    GovernanceLineageTableServiceImpl governanceLineageTableService;

    @Autowired
    GovernanceAssessTableService governanceAssessTableService;

    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Autowired
    MainAssessService mainAssessService;

    @Test
    void contextLoads() {
    }

    @Test
    public void  testInitMeta() throws Exception {
        tableMetaInfoServiceImpl.initTableMetaInfo("2023-05-02","gmall");
    }


    @Test
    public void testAllMetricAssess(){
        governanceAssessDetailService.allMetricAssess("2023-05-02");
    }

    @Test
    public  void testLineage() throws Exception {
        governanceLineageTableService.initLineage("2023-05-02");


    }

    @Test
    public void testAssessScore(){
      //  governanceAssessTableService.genGovenanceAssessTable("2023-05-02");

        //    governanceAssessTecOwnerService.genGovenanceAssessTecOwner("2023-05-02");

       // governanceAssessGlobalService.genGovenanceAssessGlobal("2023-05-02");

        mainAssessService.mainAssess("2023-05-02");
    }

}
