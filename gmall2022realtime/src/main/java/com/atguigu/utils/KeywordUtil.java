package com.atguigu.utils;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: IK分词器工具类
 */
public class KeywordUtil {
    //使用IK分词器对字符串进行分词,分开后的词为数组类型
    public static List<String> analyze(String text) {
        //IKSegmenter里面传递的参数类型时Reader类型
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, false);
        Lexeme lex = null;
        //将拆分后的string放到List集合里面
        List<String> keywordList = new ArrayList();
        while (true) {
            try {
                //如果不等于null就是下面还有值
                if ((lex = ik.next()) != null) {
                    //获取下一个值
                    String lexemeText = lex.getLexemeText();
                    //将这个值放在集合里面
                    keywordList.add(lexemeText);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return keywordList;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }
}
