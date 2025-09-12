package com.atguigu.dga_0315.governance.assessor.security;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga_0315.governance.assessor.Assessor;
import com.atguigu.dga_0315.governance.bean.AssessParam;
import com.atguigu.dga_0315.governance.bean.GovernanceAssessDetail;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Component("BEYOND_PERMISSION")
public class BeyondPermissionAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        //扫描考评表目录下的子目录及其文件 的权限 是否 超过建议权限
        // 1、  1  起点  表文件目录展开   2 工具  FileSystem 、比较参数（文件目录建议值）  3 容器  List<String>

        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObj = JSON.parseObject(metricParamsJson);
        String filePermission = paramJsonObj.getString("file_permission");
        String dirPermission = paramJsonObj.getString("dir_permission");
        String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
        String tableFsOwner = assessParam.getTableMetaInfo().getTableFsOwner();

        // 2 、对目录文件 进行遍历 递归
        // 3、权限越级的目录或文件的路径  多个值 放入  List<String>
        List<String>  beyondPermissionPathList= checkBeyondPermission(filePermission,dirPermission,tableFsPath,tableFsOwner);


        // 4、 根据List<String> 判断是否 0分  差评
        if(beyondPermissionPathList.size()>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在超越参考权限的目录："+JSON.toJSONString(beyondPermissionPathList));
        }
    }

    private List<String> checkBeyondPermission(String filePermission, String dirPermission, String tableFsPath,String fsOwner) throws Exception {
        // 1  准备递归的相关材料
        //工具
        FileSystem fileSystem = FileSystem.get(new URI(tableFsPath), new Configuration(), fsOwner);
        //容器
        List beyondPermissionPathList = new ArrayList<>();
        //起点
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(tableFsPath));

        checkBeyondPermissionRec(fileStatuses,fileSystem,beyondPermissionPathList,filePermission,   dirPermission);

        return beyondPermissionPathList;
    }

    private void checkBeyondPermissionRec(FileStatus[] fileStatuses, FileSystem fileSystem, List beyondPermissionPathList, String filePermission, String dirPermission) throws IOException {
        //1  非叶子节点
                  //  处理：    比较权限
                  //  下钻 -》 展开子节点 调用本方法
        //2   叶子节点
                 //  处理：    比较权限

        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isDirectory()){
                 boolean isBeyondPermission=  comparePermission (fileStatus.getPermission() ,dirPermission);
                 if(isBeyondPermission){
                     beyondPermissionPathList.add(fileStatus.getPath().toString());
                 }
                 FileStatus[] subFileStatuses = fileSystem.listStatus(fileStatus.getPath()); //新起点
                 checkBeyondPermissionRec(subFileStatuses,fileSystem,beyondPermissionPathList,filePermission,dirPermission); //下钻
            }else {
                boolean isBeyondPermission=  comparePermission (fileStatus.getPermission() ,filePermission);
                if(isBeyondPermission){
                    beyondPermissionPathList.add(fileStatus.getPath().toString());
                }
            }
        }
    }

    //比较一个文件或目录的权限
    private boolean comparePermission(FsPermission permission, String paramPermission) {
        int userPermission = permission.getUserAction().ordinal();  // 7
        int groupPermission = permission.getGroupAction().ordinal(); // 5
        int otherPermission = permission.getOtherAction().ordinal(); //5

        Integer paramUserPermission = Integer.valueOf( paramPermission.substring(0, 1));
        Integer paramGroupPermission = Integer.valueOf(paramPermission.substring(1, 2));
        Integer paramOtherPermission = Integer.valueOf(paramPermission.substring(2));

        if(userPermission>paramUserPermission
        ||groupPermission>paramGroupPermission
        ||otherPermission>paramOtherPermission){
            return  true;
        }

        return false;
    }
}
