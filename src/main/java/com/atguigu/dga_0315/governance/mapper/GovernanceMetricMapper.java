package com.atguigu.dga_0315.governance.mapper;

import com.atguigu.dga_0315.governance.bean.GovernanceMetric;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 考评指标参数表 Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
@Mapper
@DS("dga")
public interface GovernanceMetricMapper extends BaseMapper<GovernanceMetric> {

}
