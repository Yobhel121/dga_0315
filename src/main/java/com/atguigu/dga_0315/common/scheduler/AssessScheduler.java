package com.atguigu.dga_0315.common.scheduler;


import com.atguigu.dga_0315.governance.service.MainAssessService;
import com.atguigu.dga_0315.meta.service.TableMetaInfoService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class AssessScheduler {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    MainAssessService mainAssessService;

    @Value("${hive.schemaNames}")
    String schemaNames;

    @Scheduled(cron = "0 55 16 * * *")
    public void exec() throws Exception {
        String assessDate = DateFormatUtils.format(new Date(), "yyyy-MM-dd");
        assessDate="2023-05-02";
        String[] schemaNameArr = schemaNames.split(",");
        for (String schemaName : schemaNameArr) {
            tableMetaInfoService.initTableMetaInfo(assessDate,schemaName);
        }
        mainAssessService.mainAssess(assessDate);
    }
}
