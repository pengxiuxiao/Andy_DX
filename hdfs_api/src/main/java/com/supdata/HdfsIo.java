package com.supdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * IOUtils方式上传下载文件
 * @Author: pxx
 * @Date: 2019/3/18 21:39
 * @Version 1.0
 */
public class HdfsIo {


    Configuration conf = null;
    FileSystem fs = null;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {

        conf = new Configuration();

        fs = FileSystem.get(new URI("hdfs://192.168.2.130:9000/"), conf, "root");
    }

    /*
     * 文件上传HDFS
     *
     */
    @Test
    public void putFileToHDFS() throws IllegalArgumentException, IOException {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(new File("c:/2017级个人详细信息表.txt"));

        //2.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/2017级个人详细信息表.txt"));

        //3.流的拷贝
        IOUtils.copyBytes(fis, fos, conf);

        //4.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }

    /*
     * 文件下载HDFS
     */
    @Test
    public void getFileFromHDFS() throws IllegalArgumentException, IOException {
        //1.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hunterhenshuai"));

        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("c:/hunterhenshuai"));

        //3.流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        //4.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
