package com.atguigu.dga_0315.governance.service;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理考评结果明细 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
public interface GovernanceAssessDetailService extends IService<GovernanceAssessDetail> {


    public   void  allMetricAssess(String assessDate);
}
