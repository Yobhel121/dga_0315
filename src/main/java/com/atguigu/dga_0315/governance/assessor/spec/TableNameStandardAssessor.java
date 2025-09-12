package com.atguigu.dga_0315.governance.assessor.spec;

import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga_0315.meta.bean.TableMetaInfo;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("TABLE_NAME_STANDARD")
public class TableNameStandardAssessor  extends Assessor {

    //1 定义各层的表达式
    Pattern odsPattern = Pattern.compile("^ods_\\w+_(inc|full)$");
    Pattern dwdPattern = Pattern.compile("^dwd_\\w+_\\w+_(inc|full)$");
    Pattern dimPattern = Pattern.compile("^dim_\\w+_(zip|full)$");
    Pattern dwsPattern = Pattern.compile("^dws_\\w+_\\w+_\\w+_(1d|nd|td)$");
    Pattern adsPattern = Pattern.compile("^ads_\\w+$");
    Pattern dmPattern = Pattern.compile("^dm_\\w+$");


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {

        // 2  把待考评的表名放入对应层的表达式中
        String tableName = assessParam.getTableMetaInfo().getTableName();
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        Matcher matcher =null;
        if(dwLevel.equals("ODS")){
            matcher= odsPattern.matcher(tableName);
        }else if (dwLevel.equals("DWD")){
            matcher= dwdPattern.matcher(tableName);
        }else if (dwLevel.equals("DIM")){
            matcher= dimPattern.matcher(tableName);
        }else if (dwLevel.equals("DWS")){
            matcher= dwsPattern.matcher(tableName);
        }else if (dwLevel.equals("ADS")){
            matcher= adsPattern.matcher(tableName);
        }else if (dwLevel.equals("DM")){
            matcher= dmPattern.matcher(tableName);
        }else {
            governanceAssessDetail.setAssessScore(BigDecimal.valueOf(5));
            governanceAssessDetail.setAssessProblem("未纳入分层");
            return;
        }

        //3 执行比较
        if(!matcher.matches()){
            //4  不合格的给0分 差评
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("表名不符合规范");
        }



    }





    public static void main(String[] args) {
        //1定义表达式
        Pattern emailPattern = Pattern.compile("^\\w+@\\w{2,10}\\.(com|org|cn)$");
        //2放入要比较的字符串
        Matcher zhang3Matcher = emailPattern.matcher("zhang3@atguigu.io");
        //3 执行比较
        if(zhang3Matcher.matches()){
            System.out.println("格式正确");
        }else{
            System.out.println("格式不正确");
        }


    }
}
