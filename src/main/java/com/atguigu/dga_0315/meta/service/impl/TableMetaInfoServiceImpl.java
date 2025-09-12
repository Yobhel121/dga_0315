package com.atguigu.dga_0315.meta.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.spring.PropertyPreFilters;
import com.atguigu.dga_0315.common.util.SqlUtil;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoExtra;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoForQuery;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoVO;
import com.atguigu.dga_0315.meta.mapper.TableMetaInfoMapper;
import com.atguigu.dga_0315.meta.service.TableMetaInfoExtraService;
import com.atguigu.dga_0315.meta.service.TableMetaInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 * 元数据表 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-21
 */
@Service
@DS("dga")
public class TableMetaInfoServiceImpl extends ServiceImpl<TableMetaInfoMapper, TableMetaInfo> implements TableMetaInfoService {


    @Value("${dga.meta.url}")  // 容器启动时执行
    String metaUrl;

    HiveMetaStoreClient hiveMetaStoreClient ;

    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;


    @PostConstruct   //后置构造器  容器启动时执行，最后执行
    public   void   initMetaClient()      {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,metaUrl);
        try {
            hiveMetaStoreClient=   new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    public void  initTableMetaInfo(String assessDate,String schemaName) throws  Exception {
        // 清理当日已经存在的数据
        this.remove(new QueryWrapper<TableMetaInfo>().eq("assess_date",assessDate));


        List<String> alltableNameList = hiveMetaStoreClient.getAllTables(schemaName);

        System.out.println("alltableNameList = " + alltableNameList);

        List<TableMetaInfo> tableMetaInfoList=new ArrayList<>(alltableNameList.size());
        for (String tableName : alltableNameList) {
            //1 从hive中提取数据
            TableMetaInfo tableMetaInfo = getTableMetaFromHive(schemaName, tableName);
            //2  从hdfs中补充元数据信息
            addHdfsInfo(tableMetaInfo);
            //3  日期信息
            tableMetaInfo.setCreateTime(new Date());
            tableMetaInfo.setAssessDate(assessDate);

            tableMetaInfoList.add(tableMetaInfo);

        }
      //  System.out.println("tableMetaInfoList = " + tableMetaInfoList);
        saveBatch(tableMetaInfoList,500);
        // 初始化 辅助信息表
        tableMetaInfoExtraService.initTableMetaExtra(assessDate,tableMetaInfoList);


    }
    //补充hdfs数据
    private void addHdfsInfo(TableMetaInfo tableMetaInfo) throws  Exception {

        //递归的三个 准备工作
        // 递归起始的节点
        // 递归过程使用的工具
        // 存放递归最终结果的容器
        //1  如何从hdfs中获取数据 客户端工具？Hadoop包下的 FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(tableMetaInfo.getTableFsPath()), new Configuration(), tableMetaInfo.getTableFsOwner());
        FileStatus[] tableFilesStatus = fileSystem.listStatus(new Path(tableMetaInfo.getTableFsPath()));

        addFileInfoRec(tableFilesStatus,fileSystem,tableMetaInfo);

        tableMetaInfo.setFsCapcitySize(fileSystem.getStatus().getCapacity());
        tableMetaInfo.setFsUsedSize(fileSystem.getStatus().getUsed());
        tableMetaInfo.setFsRemainSize(fileSystem.getStatus().getRemaining());

    }
    //       非叶子节点
    //                 指向下一个子节点  调用自己的方法
    //
    //       叶子节点
    //                 处理自己业务逻辑（累加变量中）
    //                 可以返回 或者 直接结束
    private void addFileInfoRec(FileStatus[] tableFilesStatus, FileSystem fileSystem, TableMetaInfo tableMetaInfo) throws IOException {
        for (FileStatus filesStatus : tableFilesStatus) {
            if( filesStatus.isDirectory()){
                //       非叶子节点(目录)
                //                  可能也会有处理
                //                 指向下一个子节点  调用自己的方法
                FileStatus[] subFileStatus = fileSystem.listStatus(filesStatus.getPath());
                addFileInfoRec(subFileStatus,fileSystem,tableMetaInfo);
            }else{
                //       叶子节点(文件)
                //                 处理自己业务逻辑（累加变量中）
                Long tableNewSize =tableMetaInfo.getTableSize()+filesStatus.getLen();
                tableMetaInfo.setTableSize(tableNewSize);

                //结合副本数的文件大小
                long tableNewTotalSize = tableMetaInfo.getTableTotalSize() + filesStatus.getLen() * filesStatus.getReplication();
                tableMetaInfo.setTableTotalSize(tableNewTotalSize);
                // 比较 ： 比较当前文件的最后修改时间 和  之间文件的最大的最后修改时间
                if(tableMetaInfo.getTableLastModifyTime()==null){
                    tableMetaInfo.setTableLastModifyTime(new Date(filesStatus.getModificationTime()));
                }else{
                    //保留两者中较大值
                    if( tableMetaInfo.getTableLastModifyTime().compareTo(new Date(filesStatus.getModificationTime()))<0){
                        tableMetaInfo.setTableLastModifyTime(new Date(filesStatus.getModificationTime()));
                    }
                }
                // 比较 ： 比较当前文件的最后访问时间 和  之间文件的最大的最后访问时间
                if(tableMetaInfo.getTableLastAccessTime()==null){
                    tableMetaInfo.setTableLastAccessTime(new Date(filesStatus.getAccessTime()));
                }else{
                    //保留两者中较大值
                    if( tableMetaInfo.getTableLastAccessTime().compareTo(new Date(filesStatus.getAccessTime()))<0){
                        tableMetaInfo.setTableLastAccessTime(new Date(filesStatus.getAccessTime()));
                    }
                }
                //                 可以返回 或者 直接结束
            }
        }


    }

    //递归的


    //从hive中提取数据
    private    TableMetaInfo  getTableMetaFromHive(String schemaName, String tableName) throws  Exception {
        Table table = hiveMetaStoreClient.getTable(schemaName, tableName);
        System.out.println("table = " + table);
        TableMetaInfo tableMetaInfo = new TableMetaInfo();
        tableMetaInfo.setTableName(tableName);
        tableMetaInfo.setSchemaName(schemaName);

        // 过滤器 用于调整json转换过程中保留的字段
        PropertyPreFilters.MySimplePropertyPreFilter preFilter = new PropertyPreFilters().addFilter("name", "type", "comment");
        //列转换   // name  type  comment
        List<FieldSchema> fieldList = table.getSd().getCols();
        String colJsonString = JSON.toJSONString(fieldList,preFilter);
        tableMetaInfo.setColNameJson(colJsonString);
        //分区列转换
        List<FieldSchema> partitionKeyList = table.getPartitionKeys();
        String partitionColJsonString = JSON.toJSONString(partitionKeyList,preFilter);
        tableMetaInfo.setPartitionColNameJson(partitionColJsonString);

        //力气活
        // 所有者
        tableMetaInfo.setTableFsOwner( table.getOwner());
        //杂项参数
        Map<String, String> tableParameters = table.getParameters();
        String paramJson = JSON.toJSONString(tableParameters);
        tableMetaInfo.setTableParametersJson(paramJson);

        //备注
        String comment = table.getParameters().get("comment");
        tableMetaInfo.setTableComment(comment);
        //路径
        String location = table.getSd().getLocation();
        tableMetaInfo.setTableFsPath(location);

        //格式
        tableMetaInfo.setTableInputFormat(table.getSd().getInputFormat());
        tableMetaInfo.setTableOutputFormat(table.getSd().getOutputFormat());
        tableMetaInfo.setTableRowFormatSerde(table.getSd().getSerdeInfo().getSerializationLib());

        //创建日期   秒数 -->毫秒  --> Date-->   年-月-日 时：分：秒
        String createTimeStr = DateFormatUtils.format(new Date(table.getCreateTime() * 1000L), "yyyy-MM-dd HH:mm:ss");
        tableMetaInfo.setTableCreateTime(createTimeStr);

        //表类型
        tableMetaInfo.setTableType(table.getTableType());

        //分桶
        tableMetaInfo.setTableBucketColsJson(JSON.toJSONString(table.getSd().getBucketCols()));
        tableMetaInfo.setTableSortColsJson(JSON.toJSONString(table.getSd().getSortCols()));
        tableMetaInfo.setTableBucketNum(table.getSd().getNumBuckets()+0L);

        return tableMetaInfo;
    }




    // 根据查询条件和分页 来 进行列表查询
    //  mybatis-plus
//    @Override
//    public List getTableListForPage(TableMetaInfoForQuery tableMetaInfoForQuery) {
//        // 反复操作数据库 ，有大量的传输消耗 需要优化
//        List<TableMetaInfo> tableMetaInfoList = list();
//        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
//            TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfoExtraService.getOne(new QueryWrapper<TableMetaInfoExtra>()
//                    .eq("table_name", tableMetaInfo.getTableName())
//                    .eq("schema_name", tableMetaInfo.getSchemaName())
//            );
//            String tecOwnerUserName = tableMetaInfoExtra.getTecOwnerUserName();
//            String busiOwnerUserName = tableMetaInfoExtra.getBusiOwnerUserName();
//
//        }
//
//        return null;
//    }

    // 根据查询条件和分页 来 进行列表查询
    //  mybatis
    //  select xxx  from table_meta_info tm  join table_meta_info_extra te
    //  on tm.table_name=te.table_name  and  tm.schema_name=te.schema_name
    //  where  tm.schema_name  like '%%'
    //       and tm.table_name like '%xx%'
    //       and dw_level = ''
    //       and assess_date = (select max(assess_date) from table_meta_info tm1
    //       where tm.table_name =tm1.table_name and tm.schema_name =tm1.schema_name  )
    @Override
    public List getTableListForPage(TableMetaInfoForQuery tableMetaInfoForQuery) {
        StringBuilder sqlSb=new StringBuilder(200);

        sqlSb.append("select tm.id ,tm.table_name,tm.schema_name,table_comment,table_size,table_total_size,tec_owner_user_name,busi_owner_user_name, table_last_access_time,table_last_modify_time" +
                " from table_meta_info tm  join table_meta_info_extra te " +
                "     on tm.table_name=te.table_name  and  tm.schema_name=te.schema_name");
        sqlSb.append(" where assess_date = (select max(assess_date) from table_meta_info tm1\n" +
                "       where tm.table_name =tm1.table_name and tm.schema_name =tm1.schema_name  )");

        if(tableMetaInfoForQuery.getTableName()!=null&&tableMetaInfoForQuery.getTableName().trim().length()>0){
            sqlSb.append("and tm.table_name like '%").append(SqlUtil.filterUnsafeSql(tableMetaInfoForQuery.getTableName())).append("%'");
        }
        if(tableMetaInfoForQuery.getSchemaName()!=null&&tableMetaInfoForQuery.getSchemaName().trim().length()>0){
            sqlSb.append("and tm.schema_name like '%").append(SqlUtil.filterUnsafeSql(tableMetaInfoForQuery.getSchemaName())).append("%'");
        }
        if(tableMetaInfoForQuery.getDwLevel()!=null&&tableMetaInfoForQuery.getDwLevel().trim().length()>0){
            sqlSb.append("and te.dw_level = '").append(SqlUtil.filterUnsafeSql(tableMetaInfoForQuery.getDwLevel())).append("'");
        }

     // 分页  limit   xx,xx
        // 页码换算成行号   行号= （页码-1 ）* 每页行数
        int rowNo= (tableMetaInfoForQuery.getPageNo()-1)*tableMetaInfoForQuery.getPageSize();

        sqlSb.append(" limit "+rowNo+","+ tableMetaInfoForQuery.getPageSize());

        List<TableMetaInfoVO> tableMetaInfoVOList = this.baseMapper.selectTableMetaListPage(sqlSb.toString());


        return tableMetaInfoVOList;

    }

    @Override
    public Integer getTableTotalForPage(TableMetaInfoForQuery tableMetaInfoForQuery) {
        StringBuilder sqlSb=new StringBuilder(200);

        sqlSb.append("select   count(*) " +
                " from table_meta_info tm  join table_meta_info_extra te " +
                "     on tm.table_name=te.table_name  and  tm.schema_name=te.schema_name");
        sqlSb.append(" where assess_date = (select max(assess_date) from table_meta_info tm1\n" +
                "       where tm.table_name =tm1.table_name and tm.schema_name =tm1.schema_name  )");

        if(tableMetaInfoForQuery.getTableName()!=null&&tableMetaInfoForQuery.getTableName().trim().length()>0){
            sqlSb.append("and tm.table_name like '%").append(SqlUtil.filterUnsafeSql(tableMetaInfoForQuery.getTableName())).append("%'");
        }
        if(tableMetaInfoForQuery.getSchemaName()!=null&&tableMetaInfoForQuery.getSchemaName().trim().length()>0){
            sqlSb.append("and tm.schema_name like '%").append(SqlUtil.filterUnsafeSql(tableMetaInfoForQuery.getSchemaName())).append("%'");
        }
        if(tableMetaInfoForQuery.getDwLevel()!=null&&tableMetaInfoForQuery.getDwLevel().trim().length()>0){
            sqlSb.append("and te.dw_level = '").append(SqlUtil.filterUnsafeSql(tableMetaInfoForQuery.getDwLevel())).append("'");
        }


        Integer total = this.baseMapper.selectTableMetaTotalPage(sqlSb.toString());


        return total;
    }


    // 1  根据主键查询主表信息
    // 2  根据表名+库名 查询辅助信息
    @Override
    public TableMetaInfo getTableMetaInfo(Long tableId) {
        TableMetaInfo tableMetaInfo = getById(tableId);

        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfoExtraService.getOne(new QueryWrapper<TableMetaInfoExtra>()
                .eq("table_name", tableMetaInfo.getTableName())
                .eq("schema_name", tableMetaInfo.getSchemaName()));

        tableMetaInfo.setTableMetaInfoExtra(tableMetaInfoExtra);

        return tableMetaInfo;
    }

    @Override
    public List<TableMetaInfo> getTableMetaWithExtraList() {
        return  baseMapper.selectTableMetaWithExtraList();
    }

    @Override
    public Map<String, TableMetaInfo> getTableMetaWithExtraMap() {
        List<TableMetaInfo> tableMetaWithExtraList = getTableMetaWithExtraList();
        Map<String, TableMetaInfo> tableMetaInfoMap = tableMetaWithExtraList.stream().collect(Collectors.toMap(tableMetaInfo -> tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName(), tableMetaInfo -> tableMetaInfo));
        return tableMetaInfoMap;
    }

}
