package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessTable;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga_0315.governance.mapper.GovernanceAssessGlobalMapper;
import com.atguigu.dga_0315.governance.service.GovernanceAssessGlobalService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 治理总考评表 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
@Service
@DS("dga")
public class GovernanceAssessGlobalServiceImpl extends ServiceImpl<GovernanceAssessGlobalMapper, GovernanceAssessGlobal> implements GovernanceAssessGlobalService {


    public void  genGovenanceAssessGlobal(String assessDate){
        remove(new QueryWrapper<GovernanceAssessGlobal>().eq("assess_date",assessDate));

        List<GovernanceAssessGlobal> governanceAssessGlobalList =
                baseMapper.selectAssessGlobal(assessDate);
        saveBatch(governanceAssessGlobalList);
    }
}
