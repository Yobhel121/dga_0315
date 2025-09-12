package com.atguigu.dga_0315.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.common.constant.MetaConst;
import com.atguigu.dga_0315.common.util.HttpUtil;
import com.atguigu.dga_0315.ds.bean.TDsTaskInstance;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component("DATA_SKEW")
public class DataSkewAssessor extends Assessor {

    //1  从List<TaskInstance>获得当日成功的任务实例的yarn_id

    // 2 通过yarn_id 获得 成功的attempt_id
    //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090     获得attemptId  哪次尝试id是成功的

    //3 获得成功的stage的清单  stageId  List<String>
    //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090/1/stages
    //4 逐个查询stage的详细信息(task的信息)  获得 每个stage 1最耗时的task_duration  2 所有任务的总duration 3 stage的总耗时
    //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090/1/stages/1   获得某个stage的任务信息 ，通过比较任务的duration 来判断是否存在倾斜
    //5  计算每个stage的 最大耗时任务  超过 其他平均耗时的 比例

    //6   stage的总耗时 > stage_dur_seconds   用如上比例  > 参考比例     则 给0分  差评



    @Value("${spark.history.url}")
    String historyUrl=null;

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        //1  从List<TaskInstance>获得当日成功的任务实例的yarn_id


        List<TDsTaskInstance> tDsTaskInstancesList = assessParam.getTDsTaskInstancesList();
        if(tDsTaskInstancesList==null||tDsTaskInstancesList.size()==0){
            return;
        }
        TDsTaskInstance  lastSuccessTaskInstance=null;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstancesList) {
            if(tDsTaskInstance.getState().toString().equals(MetaConst.TASK_STATE_SUCCESS)){
                if(lastSuccessTaskInstance==null){
                    lastSuccessTaskInstance=tDsTaskInstance;
                }else{
                   if( lastSuccessTaskInstance.getEndTime().compareTo(tDsTaskInstance.getEndTime())<0){
                       lastSuccessTaskInstance=tDsTaskInstance;
                   }
                }
            }

        }

        String yarnId=lastSuccessTaskInstance.getAppLink();

        // 2 通过yarn_id 获得 成功的attempt_id
        //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090     获得attemptId  哪次尝试id是成功的
        String attemptId = getAttemptId(yarnId);


        //3 获得成功的stage的清单  stageId  List<String>
        //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090/1/stages
        List<Integer> stageIdList = getStageIdList(yarnId, attemptId);
        //4 逐个查询stage的详细信息(task的信息)  获得 每个stage 1最耗时的task_duration  2 所有任务的总duration 3 stage的总耗时
        // List<StageInfo>
        //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090/1/stages/1   获得某个stage的任务信息 ，通过比较任务的duration 来判断是否存在倾斜

        List<StageInfo> stageInfoList = getStageInfoList(yarnId, attemptId, stageIdList);
        //5  计算每个stage的 最大耗时任务  超过 其他平均耗时的 比例
        //6   stage的总耗时 > stage_dur_seconds   用如上比例  > 参考比例     则 给0分  差评

        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObj = JSON.parseObject(metricParamsJson);
        Integer stageDurSeconds = paramJsonObj.getInteger("stage_dur_seconds");
        BigDecimal percent = paramJsonObj.getBigDecimal("percent");


        governanceAssessDetail.setAssessComment(JSON.toJSONString(stageInfoList));
        for (StageInfo stageInfo : stageInfoList) {
             // 计算每个stage的 最大耗时任务  超过 其他平均耗时的 比例
            //     maxduration - ((sumduration -maxduration)/(tasknum-1)/  ((sumduration -maxduration)/(tasknum-1)
            Integer avgOtherDuration =  (stageInfo.sumTaskDuration-stageInfo.maxTaskDuration )/ (stageInfo.taskNum-1) ;

            BigDecimal stageBeyondPercent = BigDecimal.valueOf(stageInfo.maxTaskDuration-avgOtherDuration).movePointRight(2).divide(BigDecimal.valueOf(avgOtherDuration),2,BigDecimal.ROUND_HALF_UP);


            if(stageBeyondPercent.compareTo(percent)>0 && stageInfo.stageDuration/1000> stageDurSeconds ){
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("存在数据倾斜，倾斜百分比："+stageBeyondPercent);
                return;
            }
        }



    }

    // 2 通过yarn_id 获得 成功的attempt_id
    //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090     获得attemptId  哪次尝试id是成功的
    public  String getAttemptId(String yarnId){
        String url=historyUrl+"/"+yarnId;
        String json = HttpUtil.get(url);
        JSONObject yarnAppJsonObj = JSON.parseObject(json);

        String attemptsId=null;
        JSONArray jsonArray = yarnAppJsonObj.getJSONArray("attempts");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject attemptJsonObj = jsonArray.getJSONObject(i);
            Boolean completed = attemptJsonObj.getBoolean("completed");
            if(completed){
                attemptsId=attemptJsonObj.getString("attemptId");
                break;
            }
        }
        return attemptsId;

    }

    //3 获得成功的stage的清单  stageId  List<String>
    //http://hadoop102:18080/api/v1/applications/application_1682294710257_0090/1/stages
    private List<Integer> getStageIdList(String yarnId,String attemptId){
        String url=historyUrl+"/"+yarnId+"/"+attemptId+"/stages";
        String json = HttpUtil.get(url);

        List<JSONObject> stageJsonObjList = JSON.parseArray(json, JSONObject.class);
        //1 检查过滤   1 成功的  2 任务数>1
        //2 转换 提取stageId
        List<Integer> stageIdList = stageJsonObjList.stream().filter(stageJsonObj -> stageJsonObj.getString("status").equals("COMPLETE"))
                .filter(stageJsonObj -> stageJsonObj.getInteger("numTasks") > 1)
                .map(stageJsonObj -> stageJsonObj.getInteger("stageId")).collect(Collectors.toList());

        return stageIdList;
    }

    @Data
    class  StageInfo {
        Integer maxTaskDuration=0;

        Integer sumTaskDuration=0;

        Integer stageDuration=0;

        Integer taskNum=0;

    }

    private  List<StageInfo>  getStageInfoList(String yarnId,String attemptId,List<Integer> stageIdList){
        List<StageInfo> stageInfoList=new ArrayList<>();
        for (Integer stageId : stageIdList) {
            String url =historyUrl+"/"+yarnId+"/"+attemptId+"/stages/"+stageId;  //改Stringbuilder
            String stageJson = HttpUtil.get(url);

           JSONObject stageJsonObjSuc=null;
            JSONArray stageJsonArray = JSON.parseArray(stageJson);
            for (int i = 0; i < stageJsonArray.size(); i++) { //有可能一个stage存在多次attempt ，这里取成功的
                JSONObject stageJsonObj =  stageJsonArray.getJSONObject(i);
                if(stageJsonObj.getString("status").equals("COMPLETE")){
                    stageJsonObjSuc=stageJsonObj;
                    break;
                }
            }


            JSONObject tasksJsonObj = stageJsonObjSuc.getJSONObject("tasks");

            StageInfo stageInfo = new StageInfo();
            for (Object taskObject : tasksJsonObj.values()) {
                // 1 求最大duration
                //2 求总值

                JSONObject taskJsonObj  = (JSONObject) taskObject;
                Integer duration = taskJsonObj.getInteger("duration");
                stageInfo.setMaxTaskDuration(Math.max(duration,stageInfo.getMaxTaskDuration()));
                stageInfo.setSumTaskDuration( stageInfo.getSumTaskDuration()+ duration);

            }
            //3个数
            stageInfo.setTaskNum(tasksJsonObj.values().size());
            //4 stage_duration  存最大duration时间
            stageInfo.setStageDuration(stageInfo.getMaxTaskDuration());
            stageInfoList.add(stageInfo);
        }
        return stageInfoList;
    }
}
