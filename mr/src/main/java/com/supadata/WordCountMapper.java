package com.supadata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/3/18 21:58
 * @Version 1.0
 *
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * KEYIN：数据的起始偏移量 0~10 11~20 21~30
 *
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取数据
        String line = value.toString();

        //2.切割
        String[] words = line.split(" ");

        //3.循环的写到下一个阶段<hello,1><hunter,1>
        for (String word : words) {
            //4.输出到reduce阶段
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
