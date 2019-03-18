package com.itstaredu.hdfs04;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;


/**
 * @author Hunter
 * @date 2018��10��15�� ����8:22:21
 * @version 1.0
 * 
 * IOUtils��ʽ�ϴ������ļ�
 */
public class HdfsIo {
	
	
	Configuration conf = null;
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		
		conf = new Configuration();
		
		fs = FileSystem.get(new URI("hdfs://192.168.50.183:9000/"), conf, "root");
	}
	
	/*
	 * �ļ��ϴ�HDFS
	 * 
	 */
	@Test
	public void putFileToHDFS() throws IllegalArgumentException, IOException {
		//1.��ȡ������
		FileInputStream fis = new FileInputStream(new File("c:/2017��������ϸ��Ϣ��.txt"));
		
		//2.��ȡ�����
		FSDataOutputStream fos = fs.create(new Path("/2017��������ϸ��Ϣ��.txt"));
		
		//3.���Ŀ���
		IOUtils.copyBytes(fis, fos, conf);
		
		//4.�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		
	}
	
	/*
	 * �ļ�����HDFS
	 */
	@Test
	public void getFileFromHDFS() throws IllegalArgumentException, IOException {
		//1.��ȡ������
		FSDataInputStream fis = fs.open(new Path("/hunterhenshuai"));
		
		//2.��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("c:/hunterhenshuai"));
		
		//3.���ĶԿ�
		IOUtils.copyBytes(fis, fos, conf);
		
		//4.�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
}
