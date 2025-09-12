package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessTable;
import com.atguigu.dga_0315.governance.mapper.GovernanceAssessTableMapper;
import com.atguigu.dga_0315.governance.service.GovernanceAssessTableService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 表治理考评情况 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
@Service
@DS("dga")
public class GovernanceAssessTableServiceImpl extends ServiceImpl<GovernanceAssessTableMapper, GovernanceAssessTable> implements GovernanceAssessTableService {



    public void  genGovenanceAssessTable(String assessDate){
        remove(new QueryWrapper<GovernanceAssessTable>().eq("assess_date",assessDate));

        List<GovernanceAssessTable> governanceAssessTableList =
                baseMapper.selectAssessTableByDetail(assessDate);
        saveBatch(governanceAssessTableList);
    }
}
