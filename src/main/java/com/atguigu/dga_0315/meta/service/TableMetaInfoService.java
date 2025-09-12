package com.atguigu.dga_0315.meta.service;

import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoForQuery;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoVO;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 元数据表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-21
 */
public interface TableMetaInfoService extends IService<TableMetaInfo> {

    List<TableMetaInfoVO> getTableListForPage(TableMetaInfoForQuery tableMetaInfoForQuery);

    Integer getTableTotalForPage(TableMetaInfoForQuery tableMetaInfoForQuery);

    TableMetaInfo getTableMetaInfo(Long tableId);

    List<TableMetaInfo> getTableMetaWithExtraList();

    Map<String,TableMetaInfo> getTableMetaWithExtraMap();

    public void  initTableMetaInfo(String assessDate,String schemaName) throws  Exception;


}
