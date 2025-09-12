package com.atguigu.dga_0315.governance.service;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessTecOwner;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 技术负责人治理考评表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
public interface GovernanceAssessTecOwnerService extends IService<GovernanceAssessTecOwner> {

    public void  genGovenanceAssessTecOwner(String assessDate);
}
