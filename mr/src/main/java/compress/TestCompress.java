package compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @ClassName: TestCompress
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/25 11:37
 * @Description:
 */
public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Compress("c://2017级个人详细信息表.txt","org.apache.hadoop.io.compress.GzipCodec");
    }

    //测试压缩方法
    private static void Compress(String fileName,String method) throws ClassNotFoundException, IOException {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        Class cName = Class.forName(method);

        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(cName, new Configuration());

        //2.输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));

        //3.创建压缩输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //4.流的对考
        IOUtils.copyBytes(fis, cos, 1024*1024*2, false);

        //5.关闭资源
        fis.close();
        cos.close();
        fos.close();
    }
}
