package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.service.ReportService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ReportServiceImpl implements ReportService {
    @Override
    public void report() {

            log.info("性能报告:");

    }
}
