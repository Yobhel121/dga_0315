package com.atguigu.dga_0315.governance.mapper;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessTecOwner;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 治理总考评表 Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
@Mapper
@DS("dga")
public interface GovernanceAssessGlobalMapper extends BaseMapper<GovernanceAssessGlobal> {



    @Select("select assess_date, \n" +
            "avg(ga.score_spec_avg)  as score_spec,\n" +
            "avg(ga.score_storage_avg)  as score_storage,\n" +
            "avg(ga.score_calc_avg)  as score_calc,\n" +
            "avg(ga.score_quality_avg)  as score_quality,\n" +
            "avg(ga.score_security_avg)  as score_security,\n" +
            "avg(score_on_type_weight) score ,\n" +
            "count(*)  table_num,\n" +
            "sum(ga.problem_num) problem_num,\n" +
            "now() create_time\n" +
            "from governance_assess_table  ga where assess_date=#{assessDate} \n" +
            "group by assess_date ")
    public List<GovernanceAssessGlobal> selectAssessGlobal(@Param("assessDate") String assessDate);

}
