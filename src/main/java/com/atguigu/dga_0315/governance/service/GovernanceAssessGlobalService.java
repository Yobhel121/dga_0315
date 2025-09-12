package com.atguigu.dga_0315.governance.service;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessGlobal;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理总考评表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
public interface GovernanceAssessGlobalService extends IService<GovernanceAssessGlobal> {


    public void  genGovenanceAssessGlobal(String assessDate);
}
