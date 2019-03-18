/**
 * @Author: pxx
 * @Date: 2019/3/12 21:47
 * @Version 1.0
 */
public class TestFinally {
    public static String str = "hello";
    public static void main(String[] args) {
        System.out.println(str);
        System.out.println(f());
        System.out.println(str);
    }
    public static String f(){

        try{
            return str;

        }finally {
            str = "pxx";
        }
    }
}
