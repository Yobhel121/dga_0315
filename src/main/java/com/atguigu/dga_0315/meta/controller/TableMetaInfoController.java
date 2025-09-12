package com.atguigu.dga_0315.meta.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoExtra;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoForQuery;
import com.atguigu.dga_0315.meta.bean.TableMetaInfoVO;
import com.atguigu.dga_0315.meta.service.TableMetaInfoExtraService;
import com.atguigu.dga_0315.meta.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 元数据表 前端控制器
 * </p>
 *
 * @author zhangchen
 * @since 2023-07-21
 */
@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;

    @GetMapping("/table-list")
    public String getTableList( TableMetaInfoForQuery tableMetaInfoForQuery){   //springboot 会把键值对参数 自动封装到对象中 不需要注解

        List<TableMetaInfoVO> tableMetaInfoVOList = tableMetaInfoService.getTableListForPage(tableMetaInfoForQuery);

         Integer total= tableMetaInfoService.getTableTotalForPage(tableMetaInfoForQuery);

        Map<String,Object> resultMap=new HashMap<>();

        resultMap.put("total",total);
        resultMap.put("list",tableMetaInfoVOList);

        return JSON.toJSONString(resultMap)  ;

    }


    @GetMapping("/table/{tableId}")
    public String  getTableMeta(@PathVariable("tableId") Long tableId){

       TableMetaInfo tableMetaInfo=   tableMetaInfoService.getTableMetaInfo(  tableId);

       return  JSON.toJSONString(tableMetaInfo);

    }


    @PostMapping("/tableExtra")
    public  String saveTableExtra(@RequestBody TableMetaInfoExtra tableMetaInfoExtra){

        tableMetaInfoExtra.setUpdateTime(new Date());
        tableMetaInfoExtraService.saveOrUpdate(tableMetaInfoExtra);

        return  "success";

    }

}
