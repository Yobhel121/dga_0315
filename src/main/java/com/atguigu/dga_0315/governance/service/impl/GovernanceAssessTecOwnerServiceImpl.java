package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessTable;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga_0315.governance.mapper.GovernanceAssessTecOwnerMapper;
import com.atguigu.dga_0315.governance.service.GovernanceAssessTecOwnerService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 技术负责人治理考评表 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
@Service
@DS("dga")
public class GovernanceAssessTecOwnerServiceImpl extends ServiceImpl<GovernanceAssessTecOwnerMapper, GovernanceAssessTecOwner> implements GovernanceAssessTecOwnerService {


    public void  genGovenanceAssessTecOwner(String assessDate){
        remove(new QueryWrapper<GovernanceAssessTecOwner>().eq("assess_date",assessDate));

        List<GovernanceAssessTecOwner> governanceAssessTecOwnerList =
                baseMapper.selectAssessOwnerByTable(assessDate);
        saveBatch(governanceAssessTecOwnerList);
    }
}
