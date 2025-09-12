package com.atguigu.dga_0315.meta.service.impl;

import com.atguigu.dga_0315.common.constant.MetaConst;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoExtra;
import com.atguigu.dga_0315.meta.mapper.TableMetaInfoExtraMapper;
import com.atguigu.dga_0315.meta.service.TableMetaInfoExtraService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 * 元数据表附加信息 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-21
 */
@Service
@DS("dga")
public class TableMetaInfoExtraServiceImpl extends ServiceImpl<TableMetaInfoExtraMapper, TableMetaInfoExtra> implements TableMetaInfoExtraService {


    public  void  initTableMetaExtra(String assessDate, List<TableMetaInfo> tableMetaInfoList){
            //1 检查 要生成哪些辅助信息表  只初始化没生成的tableMetaInfoList
        List<String> tableNameList = tableMetaInfoList.stream().map(tableMetaInfo -> tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName()).collect(Collectors.toList());
          // select xxx from table_meta_info_extra where   concat(schema_name,',',table_name ) in ('gmall.dwd_order_info','gmall.ods_order_info')
        List<TableMetaInfoExtra> existsExtraList = this.list(new QueryWrapper<TableMetaInfoExtra>().in("concat(schema_name,'.',table_name)", tableNameList));
        Set<String> existsExtraSet = existsExtraList.stream().map(tableMetaInfoExtra -> tableMetaInfoExtra.getSchemaName() + "." + tableMetaInfoExtra.getTableName()).collect(Collectors.toSet());

        tableMetaInfoList.removeIf(tableMetaInfo->existsExtraSet.contains(tableMetaInfo.getSchemaName()+"."+tableMetaInfo.getTableName())  );

        List<TableMetaInfoExtra> tableMetaInfoExtraList=new ArrayList<>(tableMetaInfoList.size());
        //2  初始化 没有曾经初始化过的表
        for (TableMetaInfo tableMetaInfo: tableMetaInfoList) {
            TableMetaInfoExtra tableMetaInfoExtra = new TableMetaInfoExtra();
            tableMetaInfoExtra.setTableName(tableMetaInfo.getTableName());
            tableMetaInfoExtra.setSchemaName(tableMetaInfo.getSchemaName());

            tableMetaInfoExtra.setLifecycleType(MetaConst.LIFECYCLE_TYPE_UNSET);
            tableMetaInfoExtra.setLifecycleDays(-1L);

            tableMetaInfoExtra.setSecurityLevel(MetaConst.SECURITY_LEVEL_UNSET);
            tableMetaInfoExtra.setDwLevel(getInitDwLevelByTableName(tableMetaInfo.getTableName()));

            tableMetaInfoExtra.setCreateTime(new Date());

            tableMetaInfoExtraList.add(tableMetaInfoExtra);
        }
        //3 保存到数据库中
        saveBatch(tableMetaInfoExtraList);

    }

    private String getInitDwLevelByTableName(String tableName){
        if(tableName.startsWith("ods")){
            return "ODS";
        } else if (tableName.startsWith("dwd")) {
            return "DWD";
        }else if (tableName.startsWith("dim")) {
            return "DIM";
        }else if (tableName.startsWith("dws")) {
            return "DWS";
        }else if (tableName.startsWith("ads")) {
            return "ADS";
        }else if (tableName.startsWith("dm")) {
            return "DM";
        }else  {
            return "OTHER";
        }
    }


}
