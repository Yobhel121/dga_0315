package com.atguigu.dga_0315.governance.service.impl;

import com.atguigu.dga_0315.common.util.SpringBeanProvider;
import com.atguigu.dga_0315.ds.bean.TDsTaskDefinition;
import com.atguigu.dga_0315.ds.bean.TDsTaskInstance;
import com.atguigu.dga_0315.ds.service.TDsTaskDefinitionService;
import com.atguigu.dga_0315.ds.service.TDsTaskInstanceService;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.governance.bean.GovernanceMetric;
import com.atguigu.dga_0315.governance.mapper.GovernanceAssessDetailMapper;
import com.atguigu.dga_0315.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga_0315.governance.service.GovernanceMetricService;
import com.atguigu.dga_0315.governance.service.ReportService;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.service.TableMetaInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.security.sasl.SaslServer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * 治理考评结果明细 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-22
 */
@Service
@DS("dga")
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {


    @Autowired
    GovernanceMetricService governanceMetricService;

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    SpringBeanProvider springBeanProvider;

    @Autowired
    TDsTaskDefinitionService tDsTaskDefinitionService;

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    @Autowired
    ReportService reportService;


    ThreadPoolExecutor executor= new ThreadPoolExecutor(10,10,60, TimeUnit.SECONDS,new LinkedBlockingDeque<>());

    //1   查询出 要考评的表（最新的元数据 含辅助信息） List<TableMetaInfo>
    //2    查询出 要考评的指标列表  List<GovernanceMetric>
    //3  ds信息
    //
    //4  每张表 的 每个指标 逐一 进行考评
    //      通过两个列表的双层循环 遍历     List<GovernanceAssessDetail>
    //5  保存 到mysql数据库中
    public   void  allMetricAssess(String assessDate){
        remove( new QueryWrapper<GovernanceAssessDetail>().eq("assess_date",assessDate));


        //1   查询出 要考评的表（最新的元数据 含辅助信息） List<TableMetaInfo>
        List<TableMetaInfo> tableMetaInfoList=tableMetaInfoService.getTableMetaWithExtraList();
        Map<String, TableMetaInfo> tableMetaInfoMap = tableMetaInfoList.stream().collect(Collectors.toMap(tableMetaInfo -> tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName(), tableMetaInfo -> tableMetaInfo));

        //如何避免 循环查询数据库？ join


        //2    查询出 要考评的指标列表  List<GovernanceMetric>
        List<GovernanceMetric> governanceMetricList=governanceMetricService.list(new QueryWrapper<GovernanceMetric>().eq("is_disabled","0"));


        //3  ds信息
        //3.1  任务实例   考评日期当日
        List<TDsTaskInstance> taskInstanceList = tDsTaskInstanceService.list(new QueryWrapper<TDsTaskInstance>().eq("date_format(start_time,'%Y-%m-%d')", assessDate));
        List<Long> taskCodeList=taskInstanceList.stream().map( taskInstance->taskInstance.getTaskCode() ).collect(Collectors.toList());
        Map<String,List<TDsTaskInstance>>  taskInstanceMap= new HashMap<>(128);
        for (TDsTaskInstance tDsTaskInstance : taskInstanceList) {
            List<TDsTaskInstance> tDsTaskInstanceExistsList = taskInstanceMap.get(tDsTaskInstance.getName());
            if(tDsTaskInstanceExistsList==null){
                tDsTaskInstanceExistsList=new ArrayList<>();
            }
            tDsTaskInstanceExistsList.add(tDsTaskInstance);
            taskInstanceMap.put(tDsTaskInstance.getName(),tDsTaskInstanceExistsList);
        }


        //3.2 任务定义
        List<TDsTaskDefinition> tDsTaskDefinitionList = tDsTaskDefinitionService.getTaskDefinitionListForAssess(taskCodeList); // 1 根据taskCode从数据库中查询 任务定义 2 从task_param字段中提取sql
        Map<String, TDsTaskDefinition>  tDsTaskDefinitionMap = tDsTaskDefinitionList.stream().collect(Collectors.toMap(tDsTaskDefinition -> tDsTaskDefinition.getName(), tDsTaskDefinition -> tDsTaskDefinition));

        //4  每张表 的 每个指标 逐一 进行考评
        //      通过两个列表的双层循环 遍历     List<GovernanceAssessDetail>
        long startMs = System.currentTimeMillis();
        List<GovernanceAssessDetail> governanceAssessDetailList=new ArrayList<>(); //现货 列表
        //期货 列表
        List<CompletableFuture<GovernanceAssessDetail>> governanceAssessDetailFutureList=new ArrayList<>();
        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
            for (GovernanceMetric governanceMetric : governanceMetricList) {
                String skipAssessTables = governanceMetric.getSkipAssessTables();
                if(skipAssessTables!=null){
                    String[] skiptableArr = skipAssessTables.split(",");  //跳过白名单
                    boolean isSkip=false;
                    for (String skiptable : skiptableArr) {
                        if(tableMetaInfo.getTableName().equals(skiptable)){
                            isSkip=true;
                        }
                    }
                    if(isSkip){
                        continue;
                    }
                }


                if(tableMetaInfo.getTableMetaInfoExtra().getDwLevel().equals("DWD")){
                 //   System.out.println("1 = " + 1111);

                }
                // Assessor assesor=    governanceMetric.getMetricCode();
                Assessor assessor = springBeanProvider.getBean(governanceMetric.getMetricCode(), Assessor.class);
                AssessParam assessParam = new AssessParam();
                assessParam.setAssessDate(assessDate);
                assessParam.setGovernanceMetric(governanceMetric);
                assessParam.setTableMetaInfo(tableMetaInfo);
                assessParam.setTableMetaInfoList(tableMetaInfoList);
                assessParam.setTableMetaInfoMap(tableMetaInfoMap);
                // 任务定义 和任务实例
                TDsTaskDefinition tDsTaskDefinition = tDsTaskDefinitionMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
                assessParam.setTDsTaskDefinition(tDsTaskDefinition);

                List<TDsTaskInstance> tDsTaskInstanceList = taskInstanceMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
                assessParam.setTDsTaskInstancesList(tDsTaskInstanceList);


                //1  串行执行
               //  GovernanceAssessDetail governanceAssessDetail = assessor.metricAssess(assessParam);
                //多线程优化
                CompletableFuture<GovernanceAssessDetail> future = CompletableFuture.supplyAsync(() -> {
                    return assessor.metricAssess(assessParam);
                },executor);
                governanceAssessDetailFutureList.add(future);

               // governanceAssessDetailList.add(governanceAssessDetail);

            }

        }


        //归集 期货转现货
        governanceAssessDetailList = governanceAssessDetailFutureList.stream().map(future -> future.join()).collect(Collectors.toList());
        System.out.println("考评耗时： = " + (System.currentTimeMillis()-startMs));
        //5  保存 到mysql数据库中

        saveBatch(governanceAssessDetailList);
        reportService.report();
    }
}
