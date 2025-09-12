package com.atguigu.dga_0315.lineage.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.common.util.SqlUtil;
import com.atguigu.dga_0315.lineage.bean.GovernanceLineageTable;
import com.atguigu.dga_0315.lineage.service.GovernanceLineageTableService;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.service.TableMetaInfoService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-27
 */
@RestController
@RequestMapping("/lineage")
public class GovernanceLineageTableController {

    @Autowired
    GovernanceLineageTableService governanceLineageTableService;

    @Autowired
    TableMetaInfoService tableMetaInfoService;


    @GetMapping("/root/{tableName}")
    public  String getRoot(@PathVariable("tableName") String tableNameWithSchema){

        Map<String, TableMetaInfo> tableMetaWithExtraMap = tableMetaInfoService.getTableMetaWithExtraMap();

        String[] schemaTableArr = tableNameWithSchema.split("\\.");
        String schemaName = schemaTableArr[0];
        String tableName = schemaTableArr[1];

        QueryWrapper<GovernanceLineageTable> queryWrapper = new QueryWrapper<GovernanceLineageTable>().eq("schema_name", schemaName)
                .eq("table_name", tableName)
                .last("and governance_date = (select max(governance_date) from governance_lineage_table where schema_name='" + SqlUtil.filterUnsafeSql(schemaName) + "' and table_name='" + SqlUtil.filterUnsafeSql(tableName) + "')");
        GovernanceLineageTable lineageTable = governanceLineageTableService.getOne(queryWrapper);

        List<JSONObject> childrenList=new ArrayList<>();


        //输出表加工
        String sinkTables = lineageTable.getSinkTables();
        if(sinkTables!=null&&sinkTables.length()>0) {
            String[] sinkTableArr = sinkTables.split(",");
            for (String sinkTableName : sinkTableArr) {
                TableMetaInfo tableMetaInfo = tableMetaWithExtraMap.get(sinkTableName);
                String tableComment = tableMetaInfo.getTableComment();

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("comment", tableComment);
                jsonObject.put("id", sinkTableName);
                jsonObject.put("relation", "sink");

                childrenList.add(jsonObject);
            }
        }

        //来源表处理
        String sourceTables = lineageTable.getSourceTables();
        if(sourceTables!=null&&sourceTables.length()>0) {
            String[] sourceTableArr = sourceTables.split(",");
            for (String sourceTableName : sourceTableArr) {
                TableMetaInfo tableMetaInfo = tableMetaWithExtraMap.get(sourceTableName);
                String tableComment = tableMetaInfo.getTableComment();

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("comment", tableComment);
                jsonObject.put("id", sourceTableName);
                jsonObject.put("relation", "source");

                childrenList.add(jsonObject);
            }
        }
        TableMetaInfo tableMetaInfo = tableMetaWithExtraMap.get(tableNameWithSchema);
        String curComment=  tableMetaInfo.getTableComment();


        JSONObject rootJSONobj=new JSONObject();
        rootJSONobj.put("isRoot",true);
        rootJSONobj.put("children",childrenList);
        rootJSONobj.put("comment",curComment);
        rootJSONobj.put("id",tableNameWithSchema);

        return rootJSONobj.toJSONString();
    }

    @GetMapping("/child/{tableName}")
    public String  getChild(@PathVariable("tableName")String tableNameWithSchema , @RequestParam("relation")String relation){

        Map<String, TableMetaInfo> tableMetaWithExtraMap = tableMetaInfoService.getTableMetaWithExtraMap();

        String[] schemaTableArr = tableNameWithSchema.split("\\.");
        String schemaName = schemaTableArr[0];
        String tableName = schemaTableArr[1];

        QueryWrapper<GovernanceLineageTable> queryWrapper = new QueryWrapper<GovernanceLineageTable>().eq("schema_name", schemaName)
                .eq("table_name", tableName)
                .last("and governance_date = (select max(governance_date) from governance_lineage_table where schema_name='" + SqlUtil.filterUnsafeSql(schemaName) + "' and table_name='" + SqlUtil.filterUnsafeSql(tableName) + "')");
        GovernanceLineageTable lineageTable = governanceLineageTableService.getOne(queryWrapper);

        List<JSONObject> childrenList=new ArrayList<>();

        String childTables=null;
        if(relation.equals("sink")){
            childTables= lineageTable.getSinkTables();
        }else{
            childTables= lineageTable.getSourceTables();
        }


        if(childTables!=null&&childTables.length()>0) {
            String[] childTableArr = childTables.split(",");
            for (String childTableName : childTableArr) {
                TableMetaInfo tableMetaInfo = tableMetaWithExtraMap.get(childTableName);
                String tableComment = tableMetaInfo.getTableComment();

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("comment", tableComment);
                jsonObject.put("id", childTableName);
                jsonObject.put("relation", relation);

                childrenList.add(jsonObject);
            }
        }



        return JSON.toJSONString(childrenList) ;
    }

}
