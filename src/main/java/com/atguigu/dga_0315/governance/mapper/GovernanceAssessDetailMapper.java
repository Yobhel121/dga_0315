package com.atguigu.dga_0315.governance.mapper;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 治理考评结果明细 Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
@Mapper
@DS("dga")
public interface GovernanceAssessDetailMapper extends BaseMapper<GovernanceAssessDetail> {

}
