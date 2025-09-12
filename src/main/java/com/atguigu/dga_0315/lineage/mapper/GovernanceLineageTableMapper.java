package com.atguigu.dga_0315.lineage.mapper;

import com.atguigu.dga_0315.lineage.bean.GovernanceLineageTable;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-27
 */
@Mapper
@DS("dga")
public interface GovernanceLineageTableMapper extends BaseMapper<GovernanceLineageTable> {

}
