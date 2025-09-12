package com.atguigu.dga_0315.meta.service;

import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoExtra;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-21
 */
public interface TableMetaInfoExtraService extends IService<TableMetaInfoExtra> {

    public  void  initTableMetaExtra(String assessDate, List<TableMetaInfo> tableMetaInfoList);

 }
