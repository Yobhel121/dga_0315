package com.atguigu.dga_0315.governance.service;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessTable;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 表治理考评情况 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
public interface GovernanceAssessTableService extends IService<GovernanceAssessTable> {


    public void  genGovenanceAssessTable(String assessDate);
}
