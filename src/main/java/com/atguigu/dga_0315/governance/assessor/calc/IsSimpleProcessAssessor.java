package com.atguigu.dga_0315.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.common.util.SqlParser;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;


@Component("IS_SIMPLE_PROCESS")
public class IsSimpleProcessAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        //1  需要sql
        String sql =null;
        if(assessParam.getTDsTaskDefinition()!=null){
            sql = assessParam.getTDsTaskDefinition().getSql();
            if(sql==null){
                return;
            }
        }else{
            return;
        }


        //2  利用工具 分析sql 注入自定义节点处理器
        SimpleDispatcher simpleDispatcher = new SimpleDispatcher();
        simpleDispatcher.setDefaultSchemaName(assessParam.getTableMetaInfo().getSchemaName());

        SqlParser.parse(sql,simpleDispatcher);
        //3  从自定义节点处理器中获得
        //  1 采集sql  中复杂操作
        //  2 采集where 条件 涉及到的字段
        //  3 采集 所有被查询表 的表名
        Map<String, TableMetaInfo> tableMetaInfoMap = assessParam.getTableMetaInfoMap();

        Set<String> whereFieldSet = simpleDispatcher.getWhereFieldSet();
        Set<String> complicatedOperateSet = simpleDispatcher.getComplicatedOperateSet();
        Set<String> fromTableNameSet = simpleDispatcher.getFromTableName();


        //4  通过三项信息  ，结合 元数据  来判断是否为简单加工
        boolean isSimple=true;
        if(complicatedOperateSet.size()>0){
            isSimple=false;
        }else{
            //  1  取得所有被查询表 的元数据
            for (String tableName : fromTableNameSet) {
                TableMetaInfo tableMetaInfo = tableMetaInfoMap.get(tableName);
                if(tableMetaInfo!=null){
                    //  判断提取的字段信息是否是分区字段
                    String partcolNameJson = tableMetaInfo.getPartitionColNameJson();
                    List<JSONObject> colJsonObjList = JSON.parseArray(partcolNameJson, JSONObject.class);
                    Set<String> partitionNameSet = colJsonObjList.stream().map(colJsonObj -> colJsonObj.getString("name")).collect(Collectors.toSet());
                    if( partitionNameSet.containsAll(whereFieldSet)){ //如果所有的where 字段都属于 分区字段 则 认为是简单处理 否则不是简单处理
                        isSimple=true;
                    }else {
                        isSimple=false;
                    }
                }
            }

        }



        //5  如果是  给0分  给差评
        if(isSimple){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("简单加工");

        }else {
            if(complicatedOperateSet.size()>0){
                governanceAssessDetail.setAssessComment("包含复杂处理："+JSON.toJSONString(complicatedOperateSet));
            }
        }



        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        String partitionColNameJson = tableMetaInfo.getPartitionColNameJson();
    }

    // 自定义的节点处理器
    //  1 采集sql  中复杂操作
    //  2 采集where 条件 涉及到的字段
    //  3 采集 所有被查询表 的表名
    private class SimpleDispatcher implements Dispatcher {

        @Getter
        Set<String>  complicatedOperateSet=new HashSet<>();

        @Getter
        Set<String>  whereFieldSet=new HashSet<>();

        @Getter
        Set<String>  fromTableName=new HashSet<>();


        @Setter
        String defaultSchemaName=null;


        Set<Integer>  complicatedOperateTypeSet= Sets.newHashSet(HiveParser.TOK_JOIN,
                HiveParser.TOK_LEFTOUTERJOIN,
                HiveParser.TOK_RIGHTOUTERJOIN,
                HiveParser.TOK_FULLOUTERJOIN,
                HiveParser.TOK_GROUPBY,
                HiveParser.TOK_UNIONALL,
                HiveParser.TOK_SELECTDI,
                HiveParser.TOK_FUNCTION,
                HiveParser.TOK_FUNCTIONDI,
                HiveParser.TOK_FUNCTIONSTAR
                );


        //每到达一个节点，要处理的事情
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            ASTNode astNode = (ASTNode) nd;

            //  1 采集sql  中复杂操作
            if( complicatedOperateTypeSet.contains(astNode.getType())  ){
                complicatedOperateSet.add( astNode.getText());
            }

            //  2 采集where 条件 涉及到的字段
            //  判断逻辑：  用tok_table_or_col , 判断该节点的父亲 ，如果父亲是DOT ，则他的兄弟是字段名 ，如果不是，则他的儿子是字段名
            if(astNode.getType()==HiveParser.TOK_TABLE_OR_COL&& astNode.getAncestor(HiveParser.TOK_WHERE)!=null){
                ASTNode parentNode =  (ASTNode)astNode.getParent();
                ASTNode fieldNode =null;
                if(parentNode.getType()==HiveParser.DOT){ //如果父亲是DOT ，则取父亲二儿子
                      fieldNode =  (ASTNode) parentNode.getChild(1);
                }else{
                      fieldNode =  (ASTNode) astNode.getChild(0);  //如果父亲不是DOT ，则取他自己的儿子
                }
                String fieldName = fieldNode.getText();
                whereFieldSet.add(fieldName);
            }

            //  3 采集 所有被查询表 的表名
            if(astNode.getType()==HiveParser.TOK_TABREF ){
                ASTNode tokTableNameNode =  (ASTNode)astNode.getChild(0);
                String tableAllName =null;
                if(tokTableNameNode.getChildren().size()==1){ //不含库名
                      tableAllName =defaultSchemaName+"."+tokTableNameNode.getChild(0).getText();
                }else{  // 含库名
                    tableAllName= tokTableNameNode.getChild(0).getText()+"."+tokTableNameNode.getChild(1).getText();
                }
                fromTableName.add(tableAllName);
            }

                return null;


        }
    }


    public static void main(String[] args) {
        HashSet<String> whereField = Sets.newHashSet( "dt");
        HashSet<String> partitonField = Sets.newHashSet("dt","e");
        System.out.println(partitonField.containsAll(whereField));
    }

}
