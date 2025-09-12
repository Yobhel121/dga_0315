package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.governance.bean.GovernanceMetric;
import com.atguigu.dga_0315.governance.mapper.GovernanceMetricMapper;
import com.atguigu.dga_0315.governance.service.GovernanceMetricService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 考评指标参数表 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
@Service
@DS("dga")
public class GovernanceMetricServiceImpl extends ServiceImpl<GovernanceMetricMapper, GovernanceMetric> implements GovernanceMetricService {

}
