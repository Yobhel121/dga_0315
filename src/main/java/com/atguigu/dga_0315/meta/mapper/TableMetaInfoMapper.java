package com.atguigu.dga_0315.meta.mapper;

import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoVO;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.arrow.flatbuf.Int;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 元数据表 Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-21
 */
@Mapper
@DS("dga")
public interface TableMetaInfoMapper extends BaseMapper<TableMetaInfo> {

    @Select("${sql}")
    public List<TableMetaInfoVO> selectTableMetaListPage(@Param("sql") String sql);

    @Select("${sql}")
    public Integer selectTableMetaTotalPage(@Param("sql") String sql);


    @Select(" select tm.*,te.* ,  te.id  as te_id ,te.create_time as te_create_time \n" +
            "   from table_meta_info tm  join table_meta_info_extra te\n" +
            "             on tm.table_name=te.table_name  and  tm.schema_name=te.schema_name\n" +
            "  where assess_date = (select max(assess_date) from table_meta_info tm1\n" +
            "                   where tm.table_name =tm1.table_name and tm.schema_name =tm1.schema_name  )")
    @ResultMap("tableMetaResultMap")
    public List<TableMetaInfo> selectTableMetaWithExtraList();
}
