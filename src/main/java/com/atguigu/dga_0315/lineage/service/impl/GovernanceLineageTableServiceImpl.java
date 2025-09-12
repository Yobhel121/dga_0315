package com.atguigu.dga_0315.lineage.service.impl;

import com.atguigu.dga_0315.common.util.SqlParser;
import com.atguigu.dga_0315.ds.bean.TDsTaskDefinition;
import com.atguigu.dga_0315.ds.bean.TDsTaskInstance;
import com.atguigu.dga_0315.ds.service.TDsTaskDefinitionService;
import com.atguigu.dga_0315.ds.service.TDsTaskInstanceService;
import com.atguigu.dga_0315.lineage.bean.GovernanceLineageTable;
import com.atguigu.dga_0315.lineage.mapper.GovernanceLineageTableMapper;
import com.atguigu.dga_0315.lineage.service.GovernanceLineageTableService;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.service.TableMetaInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.Getter;
import lombok.Setter;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-27
 */
@Service
@DS("dga")
public class GovernanceLineageTableServiceImpl extends ServiceImpl<GovernanceLineageTableMapper, GovernanceLineageTable> implements GovernanceLineageTableService {


    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    @Autowired
    TDsTaskDefinitionService tDsTaskDefinitionService;

    //  1 查询所有表 任务实例
    //  2  根据任务实例查询任务的定义
    //  3  根据任务定义提取sql
    //  4  逐个分析sql    -->   map1 < 表名 , List<来源表>  >
    //                         map2< 表名 ,List<输出表>>
    // 5 获得所有表 ，进行迭代 迭代过程中查询map1  map2 获得来源表 输出表  --> List<GovernanceLineageTable>
    // 6 保存 List<GovernanceLineageTable>
    public   void initLineage(String governanceDate) throws Exception {
        //  1 查询所有表 任务实例
        //  2  根据任务实例查询任务的定义
        List<TDsTaskInstance> taskInstanceList = tDsTaskInstanceService.list(new QueryWrapper<TDsTaskInstance>().eq("date_format(start_time,'%Y-%m-%d')", governanceDate));
        List<Long> taskCodeList=taskInstanceList.stream().map( taskInstance->taskInstance.getTaskCode() ).collect(Collectors.toList());

        //3.2 任务定义
        List<TDsTaskDefinition> tDsTaskDefinitionList = tDsTaskDefinitionService.getTaskDefinitionListForAssess(taskCodeList); // 1 根据taskCode从数据库中查询 任务定义 2 从task_param字段中提取sql

        //  3  根据任务定义提取sql
        //  4  逐个分析sql    -->   map1 < 表名 , List<来源表>>
        //                         map2< 表名 ,List<输出表>>
        Map<String, Set<String>>  sourceTableMap=new HashMap<>();
        Map<String, Set<String>>  sinkTableMap=new HashMap<>();

        //元数据 map
        Map<String, TableMetaInfo> tableMetaWithExtraMap = tableMetaInfoService.getTableMetaWithExtraMap();

        for (TDsTaskDefinition tDsTaskDefinition : tDsTaskDefinitionList) {

            String sql = tDsTaskDefinition.getSql();
            if(sql==null){
                continue;
            }
            String sinkTableName = tDsTaskDefinition.getName();  //A
            //需要定义节点处理器 :  目的：获得该sql的 来源表 多个
            SourceTableDispatcher sourceTableDispatcher= new  SourceTableDispatcher();
            String[] tableNameWithSchemaArr = tDsTaskDefinition.getName().split("\\.");
            String schemaName=null;
            if(tableNameWithSchemaArr.length>0){
                schemaName= tableNameWithSchemaArr[0];
            }
            if(schemaName==null){
                continue;
            }
            sourceTableDispatcher.setDefaultSchemaName(schemaName);
            SqlParser.parse(sql,sourceTableDispatcher);
            Set<String> sourceTableNameSet = sourceTableDispatcher.getSourceTableNameSet(); //B，C,(A) ,m
            //把单次分析sql的成果 放入两个map中

            //注意处理 1 去除掉自己引用自己的情况
            sourceTableNameSet.remove(sinkTableName);
            //        2  去除掉 引用别名的情况  需要参考元数据信息 TableMetaInfoMap
            for (Iterator<String> iterator = sourceTableNameSet.iterator(); iterator.hasNext(); ) {
                String sourceTableName = iterator.next();
                TableMetaInfo tableMetaInfo = tableMetaWithExtraMap.get(sourceTableName);
                if(tableMetaInfo==null) {// 不是真表
                    iterator.remove(); //删除当前节点
                }

            }



            sourceTableMap.put(sinkTableName,sourceTableNameSet);

            for (String sourceTableName : sourceTableNameSet) {
                Set<String> sinkTableExistsSet = sinkTableMap.get(sourceTableName);  // B->F
                if(sinkTableExistsSet==null){
                    sinkTableExistsSet=new HashSet<>();
                }
                sinkTableExistsSet.add(sinkTableName);  //F+A
                sinkTableMap.put(sourceTableName,sinkTableExistsSet);  //B-> F+A
            }
        }

        System.out.println("sinkTableMap = " + sinkTableMap);


        // 5 获得所有表 ，进行迭代 迭代过程中查询map1  map2 获得来源表 输出表  --> List<GovernanceLineageTable>
        List<GovernanceLineageTable> governanceLineageTableList=new ArrayList<>(tableMetaWithExtraMap.size());

        for (TableMetaInfo tableMetaInfo : tableMetaWithExtraMap.values()) {
            GovernanceLineageTable governanceLineageTable = new GovernanceLineageTable();
            governanceLineageTable.setGovernanceDate(governanceDate);
            governanceLineageTable.setSchemaName(tableMetaInfo.getSchemaName());
            governanceLineageTable.setTableName(tableMetaInfo.getTableName());

            Set<String> sinkTableSet = sinkTableMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
            governanceLineageTable.setSinkTables(StringUtils.join(sinkTableSet,","));

            Set<String> sourceTableSet = sourceTableMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
            governanceLineageTable.setSourceTables(StringUtils.join(sourceTableSet,","));

            governanceLineageTable.setCreateTime(new Date());

            governanceLineageTableList.add(governanceLineageTable);
        }


        // 6 保存 List<GovernanceLineageTable>
        saveBatch(governanceLineageTableList);




    }

    private class SourceTableDispatcher implements Dispatcher {


        @Getter
        Set<String> sourceTableNameSet=new HashSet<>();

        @Setter
        String defaultSchemaName;

        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            //每到达一个节点 : 从tok_tabref 下取得表名
            ASTNode astNode = (ASTNode) nd;

            if(astNode.getType()== HiveParser.TOK_TABREF){
                ASTNode tokTableNameNode =(ASTNode)  astNode.getChild(0);
                String tableName=null;
                if(tokTableNameNode.getChildCount()==1){ //不带库名
                    tableName = defaultSchemaName+"."+ tokTableNameNode.getChild(0).getText();

                }else{
                    tableName = tokTableNameNode.getChild(0).getText()+"."+ tokTableNameNode.getChild(1).getText();
                }
                sourceTableNameSet.add( tableName)  ;

            }

            return null;
        }
    }
}
