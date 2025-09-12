package com.atguigu.dga_0315.ds.service;

import com.atguigu.dga_0315.ds.bean.TDsTaskDefinition;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-25
 */
public interface TDsTaskDefinitionService extends IService<TDsTaskDefinition> {

    List<TDsTaskDefinition> getTaskDefinitionListForAssess(List<Long> taskCodeList);
}
