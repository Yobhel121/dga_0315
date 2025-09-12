package com.atguigu.dga_0315.governance.mapper;

import com.atguigu.dga_0315.governance.bean.GovernanceAssessTable;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 表治理考评情况 Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-28
 */
@Mapper
@DS("dga")
public interface GovernanceAssessTableMapper extends BaseMapper<GovernanceAssessTable> {


    @Select("select assess_date,table_name,schema_name,tec_owner,\n" +
            "      avg( if( governance_type='SPEC',assess_score,null) )*10 score_spec_avg,\n" +
            "      avg( if( governance_type='STORAGE',assess_score,null) )*10 score_storage_avg,\n" +
            "      avg( if( governance_type='CALC',assess_score,null) )*10 score_calc_avg,\n" +
            "      avg( if( governance_type='QUALITY',assess_score,null) )*10 score_quality_avg,\n" +
            "      avg( if( governance_type='SECURITY',assess_score,null) )*10 score_security_avg,\n" +
            "       avg(  if( governance_type='SPEC',assess_score,null) * gt.type_weight/100)*10\n" +
            "        +avg( if( governance_type='STORAGE',assess_score,null) * gt.type_weight/100)*10\n" +
            "       + avg(if( governance_type='CALC',assess_score,null) * gt.type_weight/100)*10\n" +
            "       + avg(if( governance_type='QUALITY',assess_score,null) * gt.type_weight/100)*10\n" +
            "       + avg(if( governance_type='SECURITY',assess_score,null) * gt.type_weight /100) *10   as score_on_type_weight,\n" +
            "    sum(if(assess_score<10,1,0)) problem_num,\n" +
            "    now() create_time\n" +
            "    from governance_assess_detail  gd join governance_type gt on  gd.governance_type=gt.type_code\n" +
            "where assess_date=#{assessDate}\n" +
            "group by schema_name,table_name ,tec_owner ,assess_date ")
    public List<GovernanceAssessTable> selectAssessTableByDetail(@Param("assessDate") String assessDate);
}
