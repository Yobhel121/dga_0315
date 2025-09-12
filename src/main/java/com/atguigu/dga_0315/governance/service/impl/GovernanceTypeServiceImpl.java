package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.bean.GovernanceType;
import com.atguigu.dga_0315.governance.mapper.GovernanceTypeMapper;
import com.atguigu.dga_0315.governance.service.GovernanceTypeService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 治理考评类别权重表 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
@Service
@DS("dga")
public class GovernanceTypeServiceImpl extends ServiceImpl<GovernanceTypeMapper, GovernanceType> implements GovernanceTypeService {

}
