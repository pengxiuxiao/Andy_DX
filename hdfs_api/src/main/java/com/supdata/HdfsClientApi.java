package com.supdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: pxx
 * @Date: 2019/3/17 10:46
 * @Version 1.0
 */
public class HdfsClientApi {
    FileSystem fs = null;

    /**
     * 初始化连接hdfs
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1.加载配置
        Configuration conf = new Configuration();

        //2.设置副本数
        conf.set("dfs.replication", "2");

        //3.设置块大小
        conf.set("dfs.blocksize", "64m");
        //4.构造客户端
        fs = FileSystem.get(new URI("hdfs://192.168.2.130:9000"), conf, "root");

    }


    /**
     * 在hdfs中创建文件夹
     * @throws IOException
     */
    @Test
    public void hdfsMkdir() throws IOException {
        //1.调用创建文件夹方法
        fs.mkdirs(new Path("/pxx/123.txt"));
        //2.关闭资源
        fs.close();
    }
    /**
     * 在hdfs中创建文件
     * @throws IOException
     */
    @Test
    public void hdfsCreateNewFile() throws IOException {
        //1.调用创建文件方法
        fs.createNewFile(new Path("/pxx/1234.txt"));
        //2.关闭资源
        fs.close();
    }

    /**
     * 在hdfs中移动/修改 文件
     * @throws IOException
     */
    @Test
    public void hdfsRename() throws IOException {
        //1.调用移动并修改
        boolean rename = fs.rename(new Path("/pxx"), new Path("/peng"));
        //2.关闭资源
        fs.close();
    }

    /**
     * 在hdfs中删除文件夹
     * @throws IOException
     */
    @Test
    public void hdfsRm() throws IOException {
//        fs.delete(new Path("/pxx/123.txt"));
        //1.调用删除文件方法 参数1：路径 参数2：是否递归删除
        boolean delete = fs.delete(new Path("/pxx/123.txt"), true);
        System.out.println(delete);
        fs.close();
    }

    /**
     * 查询hdfs下指定目录的目录信息
     * @throws IOException
     */
    @Test
    public void hdfsLs() throws IOException {
        //1.调用hdfs方法 返回远程迭代器(返回文件信息 没有文件夹信息)
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);

        //2.取出迭代器数据
        while (iter.hasNext()) {
            //拿数据
            LocatedFileStatus status = iter.next();

            System.out.println("文件夹的路径为：" + status.getPath());
            System.out.println("块大小为：" + status.getBlockSize());
            System.out.println("文件长度为：" + status.getLen());
            System.out.println("块信息为：" + Arrays.toString(status.getBlockLocations()));
            System.out.println("=====================");
        }
        fs.close();
    }


    /*
     * 判断文件还是文件夹
     */
    @Test
    public void findAtHdfs() throws FileNotFoundException, IllegalArgumentException, IOException {
        //1.展示状态信息
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        //2.遍历所有文件
        for(FileStatus ls:listStatus) {
            if(ls.isFile()) {
                //文件
                System.out.println("文件----f----" + ls.getPath().getName());
            }else {
                //文件夹
                System.out.println("文件----d----" + ls.getPath().getName());
            }

        }
    }

}
