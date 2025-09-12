package com.atguigu.dga_0315.governance.assessor.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.common.constant.MetaConst;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Component("HAS_SIMILAR_TABLE")
public class HasSimilarTable  extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {
        // 1 取得percent参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObj = JSON.parseObject(metricParamsJson);
        BigDecimal paramPercent = paramJsonObj.getBigDecimal("percent");
        //过滤掉ODS
        if(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel().equals("ODS")){
            return;
        }

        // 2 获得当前考评表的元数据 ，
        TableMetaInfo curTableMetaInfo = assessParam.getTableMetaInfo();
        // 3 获取所有表的元数据列表  (尽量提前查询，通过参数传递)
        List<TableMetaInfo> otherTableMetaInfoList = assessParam.getTableMetaInfoList();
        // 4 三层循环 ，
        //   4.1 第一层，循环所有表 依次个考评考评表比较
        List<String>  similarTableNameList=new ArrayList<>();
        for (TableMetaInfo otherTableMetaInfo : otherTableMetaInfoList) {


            //排除自己
            if(curTableMetaInfo.getSchemaName().equals(otherTableMetaInfo.getSchemaName())
                    &&curTableMetaInfo.getTableName().equals(otherTableMetaInfo.getTableName())){
                continue;
            }
            //过滤掉非同层次的表
            if(!curTableMetaInfo.getTableMetaInfoExtra().getDwLevel().equals(otherTableMetaInfo.getTableMetaInfoExtra().getDwLevel())){
                continue;
            }
            //过滤掉ODS
            if(curTableMetaInfo.getTableMetaInfoExtra().getDwLevel().equals("ODS")){
                continue;
            }
            //取得字段集合  本次考评表
            String curColJson = curTableMetaInfo.getColNameJson();
            List<JSONObject> curColJsonObjList = JSON.parseArray(curColJson, JSONObject.class);
            //取得字段集合  其他表
            String otherColJson = otherTableMetaInfo.getColNameJson();
            List<JSONObject> otherColJsonObjList = JSON.parseArray(otherColJson, JSONObject.class);
            //优化 把list转为set 使用set.contains来作为比较是否相同  会少一层循环
            //    4.2 第二层 循环 考评表的所有字段
            //   4.3  第三层 每个字段和 其他表的字段比较
            Integer sameColCount=0;
            for (JSONObject curColJsonObj : curColJsonObjList) {
                for (JSONObject otherColJsonObj : otherColJsonObjList) {
                    String curColName = curColJsonObj.getString("name");
                    String otherColName = otherColJsonObj.getString("name");
                    if(curColName.equals(otherColName)){    //4.4 只要有名称相同的字段  字段相同的个数 ++
                        sameColCount++;
                    }
                }
            }

            BigDecimal  curSimilarPercent = BigDecimal.valueOf(sameColCount).movePointRight(2).divide(BigDecimal.valueOf(curColJsonObjList.size()), 2, BigDecimal.ROUND_HALF_UP);
            BigDecimal  otherSimilarPercent = BigDecimal.valueOf(sameColCount).movePointRight(2).divide(BigDecimal.valueOf(otherColJsonObjList.size()), 2, BigDecimal.ROUND_HALF_UP);
            if(curSimilarPercent.compareTo(paramPercent)>0 || otherSimilarPercent.compareTo(paramPercent)>0){
                similarTableNameList.add(otherTableMetaInfo.getTableName());   //每发现一个相似表 记录下来
            }

        }


        //   4.4 只要有名称相同的字段  字段相同的个数 ++
        //   4.5 相似比利=   字段相同的个数 /总字段个数
        // 5 相似比例 和 percent参数 作比较  如果超过 则给0分 差评
        if(similarTableNameList.size()>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在相似表：" + StringUtils.join(similarTableNameList,",")  );
        }



    }
}
