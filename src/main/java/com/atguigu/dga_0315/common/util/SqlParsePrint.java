package com.atguigu.dga_0315.common.util;


import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static guru.nidi.graphviz.model.Factory.*;

/**
 * Created by 黄凯 on 2023/6/10 0010 21:52
 *
 * @author 黄凯
 * 永远相信美好的事情总会发生.
 */
public class SqlParsePrint {

    public static void main(String[] args) throws Exception {

//        String sql = " select a,b,c from gmall.user_info  u where u.id='123' and dt='123123' ";
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
        //   自定义一个节点处理器

        String sql1=" insert overwrite table t1 select a,b,c from  t2 where a=1";
        TestDispatcher testDispatcher = new TestDispatcher();
        testDispatcher.setCount(0);


        ASTNode headAstNode = parse(testDispatcher, sql1);

        Map<String, TreeNodeBean> allASTNodeMap = testDispatcher.getAllASTNodeMap();

        TreeNodeBean treeNodeBean1 = allASTNodeMap.get(String.valueOf(headAstNode.hashCode()));

        printGraph(allASTNodeMap, treeNodeBean1);

//        System.out.println("allASTNodeMap = " + allASTNodeMap);

    }

    public static void printGraph(Map<String, TreeNodeBean> allASTNodeMap, TreeNodeBean headAstNode) throws IOException {


        MutableGraph g = mutGraph("example1").setDirected(true).use((gr, ctx) -> {

            nodeAttrs().add(Color.BLUE);
            mutNode(headAstNode.getName() + "\n" + headAstNode.getCount());
            nodeAttrs().add(Color.RED);
            nodeAttrs().add(Color.BLUE.font());

            for (Map.Entry<String, TreeNodeBean> entry : allASTNodeMap.entrySet()) {

                TreeNodeBean value = entry.getValue();

//                if (value.getName().equals("tt9")){
//
//                    System.out.println(value.getName());
//
//                }

                String name = value.getName() + "\n" + value.getCount();

                for (TreeNodeBean treeNodeBean : value.getChildrenList()) {

                    String childrenName = treeNodeBean.getName() + "\n" + treeNodeBean.getCount();

                    mutNode(name).addLink(mutNode(childrenName));

                }


            }

        });
        Graphviz.fromGraph(g).width(1600).render(Format.SVG).toFile(new File("example/sqlTree.svg"));

    }

    public static ASTNode parse(Dispatcher dispatcher, String sql) throws Exception {
        //1  把sql转换为语法树   有工具 完成  在hive依赖中就已经提供了
        ParseDriver parseDriver = new ParseDriver(); //用于把sql转为语法树
        ASTNode astNode = parseDriver.parse(sql);
        //2  提供了一个 遍历器   后序遍历
        while (astNode.getType() != HiveParser.TOK_QUERY) {   //循环遍历直到找到第一个query节点 ，循环退出 ，用query节点作为根节点。

            astNode = (ASTNode) astNode.getChild(0);
        }


        //3  自定义一个节点处理器  //根据不同的需求在方法外部定义 ，定义好后传递
        //4  把处理器放到遍历器中
        GraphWalker graphWalker = new DefaultGraphWalker(dispatcher);
        //5  让遍历器遍历语法树

        graphWalker.startWalking(Collections.singletonList(astNode), null);

        return astNode;

    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class TestDispatcher implements Dispatcher {

        private Integer count;

        //存放所有节点
        private Map<String, TreeNodeBean> allASTNodeMap = new HashMap<>();

        //每到达一个节点要处理的事项
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {

            ASTNode astNode = (ASTNode) nd;

            count++;

//            System.out.println(astNode.getToken().getText() + "\n" + count);

            TreeNodeBean treeNodeBean = new TreeNodeBean();
            treeNodeBean.setId(String.valueOf(astNode.hashCode()));
            treeNodeBean.setName(astNode.getToken().getText());
            treeNodeBean.setAstNode(astNode);
            treeNodeBean.setCount(count);
            treeNodeBean.setParent((ASTNode) astNode.getParent());

            ArrayList<Node> children = astNode.getChildren();

            if (children != null && children.size() > 0) {

                for (Node child : children) {

                    ASTNode child1 = (ASTNode) child;

                    String id = String.valueOf(child1.hashCode());

                    if (!allASTNodeMap.containsKey(id)) {

                        TreeNodeBean treeNodeBean1 = new TreeNodeBean();
                        treeNodeBean1.setId(id);
                        treeNodeBean1.setName(child1.getToken().getText());
                        treeNodeBean1.setAstNode(child1);
                        treeNodeBean1.setParent((ASTNode) child1.getParent());

                        allASTNodeMap.put(id, treeNodeBean1);

                        treeNodeBean.getChildrenList().add(treeNodeBean1);

                    } else {

                        treeNodeBean.getChildrenList().add(allASTNodeMap.get(id));

                    }

                }

            }


            if (allASTNodeMap.containsKey(treeNodeBean.getId())) {

                TreeNodeBean treeNodeBean1 = allASTNodeMap.get(treeNodeBean.getId());

                treeNodeBean1.setCount(count);

                treeNodeBean1.getChildrenList().addAll(treeNodeBean.getChildrenList());

            } else {

                allASTNodeMap.put(treeNodeBean.getId(), treeNodeBean);

            }


            return null;
        }
    }



    @Data
   static public class TreeNodeBean {

        private String id;
        private String name;

        private ASTNode astNode;

        private Integer count;

        private ASTNode parent;

        private List<TreeNodeBean> childrenList = new ArrayList<>();

    }

}
