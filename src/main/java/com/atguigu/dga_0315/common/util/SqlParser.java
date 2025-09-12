package com.atguigu.dga_0315.common.util;

import lombok.Getter;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class SqlParser {


    public static  void parse(String sql, Dispatcher dispatcher) throws  Exception {
        // 1  创建一个分析引擎 用于把sql 转为语法树
        ParseDriver parseDriver = new ParseDriver();

        // 2  转语法树
        ASTNode astNode = parseDriver.parse(sql);  //得到树的顶点
        //向下取第一个子节点
        ASTNode queryFirstNode =(ASTNode) astNode.getChild(0);

        // 3  准备进行遍历  创建一个遍历器    安装 你定制的节点处理器
        DefaultGraphWalker graphWalker = new DefaultGraphWalker(dispatcher);

        // 4  执行遍历
        graphWalker.startWalking(Collections.singletonList(queryFirstNode),null);

    }

    public static void main(String[] args) throws Exception {


        String sql = " with t1 as (select aa(a),b,c,dt as dd from tt1,  tt2 \n" +
                "             where tt1.a=tt2.b and dt='2023-05-11'  )\n" +
                "  insert overwrite table tt9  \n" +
                "  select a,b,c \n" +
                "  from t1 \n" +
                "  where    dt = date_add('${xxx}',-4 )    \n" +
                "  union \n" +
                "  select a,b,c \n" +
                "  from t2\n" +
                "   where    dt = date_add('${xxx} ',-7 )  ";

        String  sql2= "select a,b,c from abc where abc.id='123'";

        MyDispatcher myDispatcher = new MyDispatcher();
        SqlParser.parse(sql2,myDispatcher);

        if(myDispatcher.getJoinList().size()>0){
            System.out.println("该sql存在join操作   "   );
        }else {
            System.out.println("该sql不存在join操作  "   );
        }


    }

    static  class MyDispatcher implements Dispatcher{

        @Getter
        List<String> joinList =new ArrayList<>();

        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            ASTNode astNode = (ASTNode) nd;
            System.out.println("到达节点："+astNode.getText());
            if(astNode.getToken().getType()== HiveParser.TOK_JOIN){
                joinList.add(astNode.getText());
            }
            return null;
        }
    }

}
