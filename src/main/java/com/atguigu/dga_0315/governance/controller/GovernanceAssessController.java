package com.atguigu.dga_0315.governance.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga_0315.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga_0315.governance.service.GovernanceAssessGlobalService;
import com.atguigu.dga_0315.governance.service.GovernanceAssessTecOwnerService;
import com.atguigu.dga_0315.governance.service.MainAssessService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 治理考评结果明细 前端控制器
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
@RestController
@RequestMapping("/governance")
public class GovernanceAssessController {

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Autowired
    MainAssessService assessService;

    @GetMapping("/globalScore")
    public String getGlobalScore(){
        QueryWrapper<GovernanceAssessGlobal> queryWrapper = new QueryWrapper<GovernanceAssessGlobal>().inSql("assess_date", "select max(assess_date) from governance_assess_global ");
        GovernanceAssessGlobal governanceAssessGlobal = governanceAssessGlobalService.getOne(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("assessDate",governanceAssessGlobal.getAssessDate());
        jsonObject.put("sumScore",governanceAssessGlobal.getScore());

        List<BigDecimal> scoreList= new ArrayList<>();
        scoreList.add(governanceAssessGlobal.getScoreSpec());
        scoreList.add(governanceAssessGlobal.getScoreStorage());
        scoreList.add(governanceAssessGlobal.getScoreCalc());
        scoreList.add(governanceAssessGlobal.getScoreQuality());
        scoreList.add(governanceAssessGlobal.getScoreSecurity());
        jsonObject.put("scoreList",scoreList);

        return jsonObject.toJSONString();


    }

    @GetMapping("/rankList")
    public  String getRankList(){

        QueryWrapper<GovernanceAssessTecOwner> queryWrapper = new QueryWrapper<GovernanceAssessTecOwner>()
                .inSql("assess_date", "select max(assess_date) from governance_assess_tec_owner ")
                .orderByDesc("score");

        List<GovernanceAssessTecOwner> governanceAssessTecOwnerList = governanceAssessTecOwnerService.list(queryWrapper);
        List<JSONObject> resultList=new ArrayList<>();
        for (GovernanceAssessTecOwner governanceAssessTecOwner : governanceAssessTecOwnerList) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("tecOwner",governanceAssessTecOwner.getTecOwner()!=null?governanceAssessTecOwner.getTecOwner():"未填写");
            jsonObject.put("score",governanceAssessTecOwner.getScore());

            resultList.add(jsonObject);
        }

        return JSONObject.toJSONString(resultList);

    }

    @GetMapping("/problemList/{governType}/{pageNo}/{pageSize}")
    public String getProblemList(@PathVariable("governType") String governType ,
                                  @PathVariable("pageNo") int pageNo ,
                                  @PathVariable("pageSize") int pageSize  ){
        int rowNo= (pageNo-1)*pageSize;
        QueryWrapper<GovernanceAssessDetail> queryWrapper = new QueryWrapper<GovernanceAssessDetail>()
                .eq("governance_type", governType)
                .inSql("assess_date", "select max(assess_date) from governance_assess_detail ")
                .last(" limit  " + rowNo + ", " + pageSize);
        List<GovernanceAssessDetail> governanceAssessDetailList = governanceAssessDetailService.list(queryWrapper);

        return JSON.toJSONString(governanceAssessDetailList);
    }

    @GetMapping("/problemNum")
    public String getProblemNum(){
        QueryWrapper<GovernanceAssessDetail> queryWrapper = new QueryWrapper<GovernanceAssessDetail>()
                .select("governance_type","count(*) ct")
                .lt("assess_score",10)
                .inSql("assess_date", "select max(assess_date) from governance_assess_detail ")
                        .groupBy("governance_type");


        List<Map<String, Object>> mapList = governanceAssessDetailService.listMaps(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        for (Map<String, Object> map : mapList) {
            String governanceType = (String)map.get("governance_type");
            Long ct = (Long)map.get("ct");
            jsonObject.put(governanceType,ct);
        }


        return jsonObject.toJSONString() ;


    }

    @PostMapping("/assess/{assessDate}")
    public String assess(@PathVariable("assessDate") String assessDate){
        assessService.mainAssess(assessDate);
        return "success";
    }
}
