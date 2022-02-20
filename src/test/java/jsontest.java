import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName jsontest
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/7 11:38
 * @Version 1.0
 */
public class jsontest {
    public static void main(String[] args) {
        //json字符串-简单对象型
        String  JSON_OBJ_STR = "{\"studentName\":\"lily\",\"studentAge\":12}";
//json字符串-数组类型
        String  JSON_ARRAY_STR = "[{\"studentName\":\"lily\",\"studentAge\":12},{\"studentName\":\"lucy\",\"studentAge\":15}]";
//复杂格式json字符串
        String  COMPLEX_JSON_STR = "{\"teacherName\":\"crystall\",\"teacherAge\":27,\"course\":{\"courseName\":\"english\",\"code\":1270},\"students\":[{\"studentName\":\"lily\",\"studentAge\":12},{\"studentName\":\"lucy\",\"studentAge\":15}]}";
        //将json字符串格式转换成json对象
        JSONObject jsonObject = JSON.parseObject(JSON_OBJ_STR);
        String studentName = jsonObject.getString("studentName");
        System.out.println(studentName);
    }
}

