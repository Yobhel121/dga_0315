package com.atguigu.dga_0315.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.dga_0315.common.util.SqlParser;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.google.common.collect.Sets;
import lombok.*;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;

@Component("MULTI_PARTITION")
public class MultiPartitionAssessor extends Assessor {

    @Override
     public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        if(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel().equals("ODS") ||assessParam.getTDsTaskDefinition()==null){  //ods层没有sql处理
            return;
        }

        //跨分区扫描
        //提取sql 进行
//        if(assessParam.getTableMetaInfo().getTableName().equals("ads_order_to_pay_interval_avg")){
//            System.out.println(111);
//        }



        String sql = assessParam.getTDsTaskDefinition().getSql();
        governanceAssessDetail.setAssessComment( sql);
        Map<String, TableMetaInfo> tableMetaInfoMap = assessParam.getTableMetaInfoMap();

        CheckMultiPartitionScanDispatcher dispatcher = new CheckMultiPartitionScanDispatcher();
        dispatcher.setTableMetaInfoMap(tableMetaInfoMap);
        dispatcher.setDefaultSchemaName(assessParam.getTableMetaInfo().getSchemaName());
        SqlParser.parse(sql, dispatcher);

        Map<String,Set<String>>  tableFilterFieldMap=new HashMap<>();
        Map<String,Set<String>>  tableRangeFilterFieldMap=new HashMap<>();


        //把条件列表 整理为 表-被过滤字段 的结构
        List<CheckMultiPartitionScanDispatcher.WhereCondition> whereConditionList = dispatcher.getWhereConditionList();
        for (CheckMultiPartitionScanDispatcher.WhereCondition whereCondition : whereConditionList) {

                List<CheckMultiPartitionScanDispatcher.OriginTableField> tableFieldList = whereCondition.getTableFieldList();

                for (CheckMultiPartitionScanDispatcher.OriginTableField originTableField : tableFieldList) {
                    Set<String> tableFilterFieldSet = tableFilterFieldMap.get(originTableField.getOriginTable());
                    if(tableFilterFieldSet==null){
                        tableFilterFieldSet=new HashSet<>();
                        tableFilterFieldMap.put( originTableField.getOriginTable(),tableFilterFieldSet);
                    }
                    tableFilterFieldSet.add(originTableField.getField());  //保存所有过滤的字段

                    if(!whereCondition.operator.equals("=")){
                        Set<String> tableFilterFilterFieldSet = tableRangeFilterFieldMap.get(originTableField.getOriginTable());
                        if(tableFilterFilterFieldSet==null){
                            tableFilterFilterFieldSet=new HashSet<>();
                            tableRangeFilterFieldMap.put(originTableField.getOriginTable(),tableFilterFieldSet);
                        }
                        tableFilterFilterFieldSet.add(originTableField.getField()); //保存范围过滤的字段

                    }
                }
        }

        StringBuilder assessProblem=new StringBuilder();
        // 获得所有引用表的清单
        Map<String, List<CheckMultiPartitionScanDispatcher.CurTableField>> refTableFieldMap = dispatcher.getRefTableFieldMap();
        for (String refTableName : refTableFieldMap.keySet()) {
            //获得元数据的分区字段
            TableMetaInfo tableMetaInfo = tableMetaInfoMap.get(refTableName);
            Set<String> tableFilterFieldSet = tableFilterFieldMap.get(refTableName);
            Set<String> tableFilterRangeFieldSet = tableRangeFilterFieldMap.get(refTableName);

            //检查每个分区字段 1 是否被过滤  2 是否被范围查询
            String partitionColNameJson = tableMetaInfo.getPartitionColNameJson();
            List<JSONObject> partitionJsonObjList = JSON.parseArray(partitionColNameJson, JSONObject.class);
            for (JSONObject partitionJsonObj : partitionJsonObjList) {
                String partitionName = partitionJsonObj.getString("name");
                if(tableFilterFieldSet==null || !tableFilterFieldSet.contains(partitionName)){
                    assessProblem.append("引用表:"+refTableName+"中的分区字段"+partitionName+"未参与过滤 ;");
                }
                if(tableFilterRangeFieldSet!=null && tableFilterRangeFieldSet.contains(partitionName)){
                    assessProblem.append("引用表:"+refTableName+"中的分区字段"+partitionName+"涉及多分区扫描 ;");
                }
            }

        }

        if(assessProblem.length()>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem(assessProblem.toString());
            governanceAssessDetail.setAssessComment( JSON.toJSONString(dispatcher.refTableFieldMap.keySet()) +"||"  +sql);

        }



    }




    //节点处理器 会经过sql所有节点处理环节，每经过一个节点执行dispatch方法
    public class CheckMultiPartitionScanDispatcher implements Dispatcher {

        //检查策略
        // 1  获得比较条件的语句
        // 2  查看比较字段是否为分区字段
        // 3  如果比较符号为 >=  <= < >  <> in 则是为多分区
        // 4  如果比较符号为 =  则获得比较的值 如果同一个分区字段 有多个值 则视为多分区


        @Setter
        Map<String, TableMetaInfo> tableMetaInfoMap = new HashMap<>();



        //用于临时存放子查询的输出结果
        Map<String, List<CurTableField>> subQueryTableFieldMap = new HashMap<>();   //表<表名,<字段名,Set<原始字段>>


        //用于存放最终写入目标表的字段
        Map<String, List<CurTableField>> insertTableFieldMap = new HashMap<>();   //表<表名,<字段名,Set<原始字段>>


        //用于临时存放被查询来源表及其字段
        @Getter
        Map<String,List<CurTableField>>  refTableFieldMap= new HashMap<>();

        //用于存放where子句的各个条件表达式
        @Getter
        List<WhereCondition> whereConditionList = new ArrayList<>();
        @Setter
        String defaultSchemaName = null;

        Set<String> operators = Sets.newHashSet("=", ">", "<", ">=", "<=", "<>", "like"); // in / not in 属于函数计算

        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {

            //检查该节点的处理内容
            ASTNode queryNode = (ASTNode) nd;
            //分析查询
            if (queryNode.getType() == HiveParser.TOK_QUERY) {

                //System.out.println("astNode = " + queryNode.getText());
                Map<String, List<CurTableField>> curQueryTableFieldMap = new HashMap<>();
                Map<String, String> aliasMap = new HashMap<>();



                for (Node childNode : queryNode.getChildren()) {

                    ASTNode childAstNode = (ASTNode) childNode;
                    if (childAstNode.getType() == HiveParser.TOK_FROM) {
                        loadTablesFromNodeRec(childAstNode, curQueryTableFieldMap, aliasMap);
                    } else if (childAstNode.getType() == HiveParser.TOK_INSERT) {
                        for (Node insertChildNode : childAstNode.getChildren()) {
                            ASTNode insertChildAstNode = (ASTNode) insertChildNode;
                            if (insertChildAstNode.getType() == HiveParser.TOK_WHERE) { //把where语句中的子句存放到list中
                                loadConditionFromNodeRec(insertChildAstNode, whereConditionList, curQueryTableFieldMap, aliasMap);
                            } else if (insertChildAstNode.getType() == HiveParser.TOK_SELECT||insertChildAstNode.getType() == HiveParser.TOK_SELECTDI) {
                                // 如果有子查询 //把查询字段写入缓存 // 没有子查询作为最终输出字段
                                List<CurTableField> tableFieldOutputList = getTableFieldOutput(insertChildAstNode, curQueryTableFieldMap, aliasMap );
                                //向上追溯子查询的别名
                                ASTNode subqueryNode = (ASTNode) queryNode.getAncestor(HiveParser.TOK_SUBQUERY);
                                //保存到子查询
                                if(subqueryNode!=null){
                                    cacheSubqueryTableFieldMap(subqueryNode,tableFieldOutputList);
                                }
                                ASTNode insertTableNode =(ASTNode) childAstNode.getFirstChildWithType(HiveParser.TOK_DESTINATION).getChild(0);
                                if(insertTableNode.getType()==HiveParser.TOK_TAB){  //需要做sql最终输出
                                    cacheInsertTableField(  insertTableNode , tableFieldOutputList);
                                }

                            }
                        }

                    }
                }
            }


            return null;
        }

        private void loadTableFiledByTableName(String tableName, Map<String, List<CurTableField>> curTableFieldMap, Map<String, String> aliasMap) {
            String tableWithSchema = tableName;
            if (tableName.indexOf(".") < 0) {
                tableWithSchema = defaultSchemaName + "." + tableName;
            }
            TableMetaInfo tableMetaInfo = tableMetaInfoMap.get(tableWithSchema);
            if (tableMetaInfo != null) {  //是 真实表
                List<CurTableField> curFieldsList = new ArrayList<>();
                //加载普通字段
                String colNameJson = tableMetaInfo.getColNameJson();
                List<JSONObject> colJsonObjectList = JSON.parseArray(colNameJson, JSONObject.class);

                for (JSONObject colJsonObject : colJsonObjectList) {
                     CurTableField curTableField = new CurTableField();

                     OriginTableField originTableField = new OriginTableField();
                     originTableField.setField(colJsonObject.getString("name"));
                     originTableField.setPartition(false);
                     originTableField.setOriginTable(tableMetaInfo.getSchemaName()+"."+tableMetaInfo.getTableName());
                     if(colJsonObject.getString("type").indexOf("struct")>0) {
                         Set<String> structFieldSet = getStructFieldSet(colJsonObject.getString("type"));
                         originTableField.setSubFieldSet(structFieldSet);
                     }

                    curTableField.getOriginTableFieldList().add(originTableField);
                    curTableField.setCurFieldName(colJsonObject.getString("name"));
                    curFieldsList.add(curTableField);
                }
                //加载分区字段
                String partitionColNameJson = tableMetaInfo.getPartitionColNameJson();
                List<JSONObject> partitionJsonObjectList = JSON.parseArray(partitionColNameJson, JSONObject.class);
                for (JSONObject partitionColJsonObject : partitionJsonObjectList) {
                    CurTableField curTableField = new CurTableField();

                    OriginTableField originTableField = new OriginTableField();
                    originTableField.setField(partitionColJsonObject.getString("name"));
                    originTableField.setPartition(true);
                    originTableField.setOriginTable(tableMetaInfo.getSchemaName()+"."+tableMetaInfo.getTableName());

                    curTableField.getOriginTableFieldList().add(originTableField);
                    curTableField.setCurFieldName(partitionColJsonObject.getString("name"));
                    curFieldsList.add(curTableField);
                }

                curTableFieldMap.put(tableName, curFieldsList);
                refTableFieldMap.put(tableName, curFieldsList);
                String tableWithoutSchema = tableWithSchema.substring(tableWithSchema.indexOf(".") + 1);
                aliasMap.put(tableWithoutSchema, tableWithSchema);  //把不带库名的表名 作为别名的一种
            } else {  //不是真实表 从缓存中提取
                List<CurTableField> subqueryFieldsList = subQueryTableFieldMap.get(tableName);
                if (subqueryFieldsList == null) {
                    throw new RuntimeException("未识别对应表: " + tableName);
                }
                curTableFieldMap.put(tableName, subqueryFieldsList);

            }


        }

        // 递归查找某个节点下的引用的表，并进行加载
        public void loadTablesFromNodeRec(ASTNode astNode, Map<String, List<CurTableField>> tableFieldMap, Map<String, String> aliasMap) {
            if (astNode.getType() == HiveParser.TOK_TABREF) {
                ASTNode tabTree = (ASTNode) astNode.getChild(0);
                String tableName = null;
                if (tabTree.getChildCount() == 1) {
                    tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0));
                } else {
                    tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);  //自动拼接表名
                }
                //根据表名和补充元数据
                loadTableFiledByTableName(tableName, tableFieldMap, aliasMap);

                //涉及别名
                if (astNode.getChildren().size() == 2) {
                    ASTNode aliasNode = (ASTNode) astNode.getChild(1);
                    aliasMap.put(aliasNode.getText(), tableName);
                }

            }else if(astNode.getType() == HiveParser.TOK_SUBQUERY){
                String aliasName = astNode.getFirstChildWithType(HiveParser.Identifier).getText();
                loadTableFiledByTableName(aliasName, tableFieldMap, aliasMap);
            } else if (astNode.getChildren() != null && astNode.getChildren().size() > 0) {
                for (Node childNode : astNode.getChildren()) {
                    ASTNode childAstNode = (ASTNode) childNode;
                    loadTablesFromNodeRec(childAstNode, tableFieldMap, aliasMap);
                }
            }
        }


        //递归检查并收集条件表达式
        public void loadConditionFromNodeRec(ASTNode node, List<WhereCondition> whereConditionList, Map<String, List<CurTableField>> queryTableFieldMap, Map<String, String> aliasMap) {

            if (operators.contains(node.getText())
                    ||(node.getType()==HiveParser.TOK_FUNCTION && node.getChild(0).getText().equals("in") )  ) {
                WhereCondition whereCondition = new WhereCondition();
                if( node.getType()==HiveParser.TOK_FUNCTION && node.getChild(0).getText().equals("in")){
                   if( node.getParent().getText().equals("not")) {
                       whereCondition.setOperator("nin");
                   }else {
                       whereCondition.setOperator("in");
                   }
                }else{
                    whereCondition.setOperator(node.getText());
                }

                ArrayList<Node> children = node.getChildren();
                for (Node child : children) {
                    ASTNode operatorChildNode = (ASTNode) child;
                    if (operatorChildNode.getType() == HiveParser.DOT) {   //带表名的字段名
                        ASTNode prefixNode = (ASTNode) operatorChildNode.getChild(0).getChild(0);
                        ASTNode fieldNode = (ASTNode) operatorChildNode.getChild(1);
                        getWhereField(whereCondition, prefixNode.getText(), fieldNode.getText(), queryTableFieldMap, aliasMap);
                        whereConditionList.add(whereCondition);
                    } else if (operatorChildNode.getType() == HiveParser.TOK_TABLE_OR_COL) {  //不带表名的字段名
                        ASTNode fieldNode = (ASTNode) operatorChildNode.getChild(0);
                        getWhereField(whereCondition, null, fieldNode.getText(), queryTableFieldMap, aliasMap);
                        whereConditionList.add(whereCondition);
                    }
                }
            } else {
                if (node.getChildren() != null) {
                    for (Node nd : node.getChildren()) {
                        ASTNode nodeChild = (ASTNode) nd;
                        loadConditionFromNodeRec(nodeChild, whereConditionList, queryTableFieldMap, aliasMap);
                    }
                }
            }
        }

        private String getInputTableName(Stack<Node> stack) {
            ASTNode globalQueryNode = (ASTNode) stack.firstElement();
            ASTNode insertNode = (ASTNode) globalQueryNode.getFirstChildWithType(HiveParser.TOK_INSERT);
            ASTNode tableNode = (ASTNode) insertNode.getChild(0).getChild(0);  //TOK_DESINATION->TOK_TAB
            if (tableNode.getChildCount() == 1) {
                return defaultSchemaName + "." + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tableNode.getChild(0));      //不带库名 补库名
            } else {
                return BaseSemanticAnalyzer.getUnescapedName((ASTNode) tableNode.getChild(0)) + "." + tableNode.getChild(1);  //带库名
            }
        }


        public List<CurTableField>   getTableFieldOutput(ASTNode selectNode, Map<String, List<CurTableField>> curQueryTableFieldMap, Map<String, String> aliasMap ) {

            List<CurTableField> outputTableFieldList = new ArrayList<>();
            for (Node selectXPRNode : selectNode.getChildren()) {
                ASTNode selectXPRAstNode = (ASTNode) selectXPRNode;
                // 如果是隐性 或者select * //返回子查询
                if(selectXPRAstNode.getChild(0).getType()==HiveParser.TOK_SETCOLREF || selectXPRAstNode.getChild(0).getType()==HiveParser.TOK_ALLCOLREF  ){
                    for (List<CurTableField> curTableFieldList : curQueryTableFieldMap.values()) {
                        outputTableFieldList.addAll(curTableFieldList);
                    }
                    return  outputTableFieldList;
                }else{
                    //逐个取节点下的select 的字段
                    CurTableField curTableField = new CurTableField();
                    loadCurTableFieldFromNodeRec((ASTNode) selectXPRAstNode ,curTableField, curQueryTableFieldMap, aliasMap);
                    if (selectXPRNode.getChildren().size() == 2) { //说明为字段起了别名
                        ASTNode aliasNode = (ASTNode) ((ASTNode) selectXPRNode).getChild(1);
                        curTableField.setCurFieldName(aliasNode.getText());
                    }
                    outputTableFieldList.add(curTableField);

                }
            }
            return  outputTableFieldList;

        }

        //把对象保存到子查询缓存中
        private void cacheSubqueryTableFieldMap(ASTNode subqueryNode,List<CurTableField> curTableFieldList  ){
            ASTNode subqueryAliasNode = (ASTNode) subqueryNode.getFirstChildWithType(HiveParser.Identifier);
            String aliasName = subqueryAliasNode.getText();
            List<CurTableField> existsTableFieldList = subQueryTableFieldMap.get(aliasName);

            if (existsTableFieldList != null) { //说明已经有查询声明为改别名了 ，主要原因是因为union造成的， 这种情况要按照顺序把每个字段的原始字段信息追加
                for (int i = 0; i < existsTableFieldList.size(); i++) {
                    CurTableField existsTableField = existsTableFieldList.get(i);
                    CurTableField curTableField = curTableFieldList.get(i);
                    existsTableField.getOriginTableFieldList().addAll(curTableField.getOriginTableFieldList());
                }

            } else {          //把子查询加入缓存
                subQueryTableFieldMap.put(aliasName, curTableFieldList);
            }
        }


        private void cacheInsertTableField(ASTNode outputTableNode,List<CurTableField> curTableFieldList ){
            String outputTableName=null;
            if( outputTableNode.getChildCount()==2){
                outputTableName= outputTableNode.getChild(0).getChild(0).getText()+"."+outputTableNode.getChild(1).getText();
            }else{
                outputTableName= defaultSchemaName+"."+outputTableNode.getChild(0).getChild(0).getText();
            }

            insertTableFieldMap.put(outputTableName,curTableFieldList);
        }


        // 利用递归获得当前节点下的字段信息
        public void loadCurTableFieldFromNodeRec(ASTNode  recNode,CurTableField curTableField,  Map<String, List<CurTableField>> curQueryTableFieldMap, Map<String, String> aliasMap) {
            if (recNode.getChildren() != null) {
                for (Node subNode : recNode.getChildren()) {
                    ASTNode subAstNode = (ASTNode) subNode;
                    if (subAstNode.getType() == HiveParser.DOT) {  //带表的字段
                        ASTNode prefixNode = (ASTNode) subAstNode.getChild(0).getChild(0);
                        ASTNode fieldNode = (ASTNode) subAstNode.getChild(1);
                        String prefix = prefixNode.getText();
                        List<OriginTableField>  originTableFieldList = getOriginFieldByFieldName(prefix, fieldNode.getText(),curQueryTableFieldMap, aliasMap);


                        curTableField.getOriginTableFieldList().addAll(originTableFieldList) ;
                        curTableField.setCurFieldName(fieldNode.getText());


                    } else if (subAstNode.getType() == HiveParser.TOK_TABLE_OR_COL) {
                        ASTNode fieldNode = (ASTNode) subAstNode.getChild(0);
                        //不带表的字段要从
                        List<OriginTableField> originTableFieldList = getOriginTableFieldList(curQueryTableFieldMap, fieldNode.getText());
                        curTableField.getOriginTableFieldList().addAll(originTableFieldList) ;
                        curTableField.setCurFieldName(fieldNode.getText());


                    } else {
                        loadCurTableFieldFromNodeRec(subAstNode,curTableField, curQueryTableFieldMap, aliasMap);

                    }

                }
            }


        }

        //前缀
        public void getWhereField(WhereCondition whereCondition, String prefix, String fieldName, Map<String, List<CurTableField>> queryTableFieldMap, Map<String, String> aliasMap) {

            List<OriginTableField> originTableFieldList = null;
            if (prefix == null) {
                originTableFieldList= getOriginTableFieldList( queryTableFieldMap,   fieldName);
            } else {   //有前缀
                originTableFieldList = getOriginFieldByFieldName(prefix, fieldName,queryTableFieldMap, aliasMap);   //把前缀作为表查询

            }


            if (originTableFieldList == null) {
                throw new RuntimeException("无法识别的字段名：" + fieldName);
            }
            whereCondition.setTableFieldList(originTableFieldList);
        }


//        private List<OriginTableField> getOriginTableFieldList(List<CurTableField> curTableField, String fieldName) {
//
//        }


//        private List<CurTableField> getCurTableFieldListByPrefix(String prefix, Map<String, List<CurTableField>> queryTableFieldMap, Map<String, String> aliasMap) {
//            List<CurTableField> curFieldList = queryTableFieldMap.get(prefix);   //把前缀作为表查询
//            if (curFieldList == null) {//未查询出 尝试换为字段查询
//                String tableName = aliasMap.get(prefix);
//                if (tableName != null) {
//                    curFieldList = queryTableFieldMap.get(tableName);
//                }
//            }
//
//            return curFieldList;
//        }



        //根据前缀和字段名 获得从表结构中获得 原始字段列表
        private   List<OriginTableField> getOriginFieldByFieldName(String prefix,String fieldName,Map<String, List<CurTableField>> queryTableFieldMap , Map<String, String> aliasMap){

            List<CurTableField> curFieldList = queryTableFieldMap.get(prefix);   //把前缀作为表查询
            if (curFieldList == null) {//未查询出 尝试换为字段查询
                String tableName = aliasMap.get(prefix);
                if (tableName != null) {
                    curFieldList = queryTableFieldMap.get(tableName);
                }
            }
            if (curFieldList == null) {
               return getOriginTableFieldList(  queryTableFieldMap,   prefix); //前缀有可能是结构体字段名
            }
            if(curFieldList==null){
                throw new RuntimeException("不明确的表前缀：" + prefix);
            }
            return  getOriginTableFieldList(  fieldName ,  curFieldList);

        }


        private List<OriginTableField>  getOriginTableFieldList(String fieldName , List<CurTableField> curFieldList){
            for (CurTableField tableField : curFieldList) {
                if (tableField.getCurFieldName().equals(fieldName)) {
                    return tableField.getOriginTableFieldList();
                }
            }
            return  new ArrayList<>();  // 一般是常量字段 比 lateral产生的常量字段 不是从表中计算而来
        }

        private List<OriginTableField> getOriginTableFieldList(Map<String, List<CurTableField>> queryTableFieldMap, String fieldName) {

            List<OriginTableField> originTableFieldList = null;

            for (Map.Entry entry : queryTableFieldMap.entrySet()) {

                List<CurTableField> curTableFieldList = (List<CurTableField>) entry.getValue();
                List<OriginTableField> matchedOriginTableFieldList =getOriginTableFieldList(fieldName,curTableFieldList );
                if (originTableFieldList != null&&originTableFieldList.size()>0 && matchedOriginTableFieldList.size()>0  ) {
                    throw new RuntimeException("归属不明确的字段：" + fieldName);
                }else  {
                    originTableFieldList=matchedOriginTableFieldList;
                }
            }
            return originTableFieldList;
        }


        //拆分子字段
        //struct<ar:string,ba:string,ch:string,is_new:string,md:string,mid:string,os:string,sid:string,uid:string,vc:string>
        private Set<String>  getStructFieldSet(String structType  )  {
            Set<String>  subFieldNameSet = new HashSet();
            structType = structType.replace("struct<", "").replace(">", "");
            String[] fieldArr = structType.split(",");
            for (String fieldString : fieldArr) {
                String[] field = fieldString.split(":");
                String fieldName = field[0];
                subFieldNameSet.add(fieldName);
            }
            return  subFieldNameSet;
        }


        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        class CurTableField {
            String curFieldName;
            List<OriginTableField> originTableFieldList =new ArrayList<>();
        }

        @Data
        class OriginTableField {
            String field;
            String originTable;
            Set<String> subFieldSet;
            boolean isPartition;


        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        class WhereCondition {
            List<OriginTableField> tableFieldList = new ArrayList<>();
            String operator = null;
        }
    }
}
