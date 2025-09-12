package com.atguigu.dga_0315.ds.mapper;

import com.atguigu.dga_0315.ds.bean.TDsTaskDefinition;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-25
 */
@Mapper
@DS("ds")
public interface TDsTaskDefinitionMapper extends BaseMapper<TDsTaskDefinition> {

}
