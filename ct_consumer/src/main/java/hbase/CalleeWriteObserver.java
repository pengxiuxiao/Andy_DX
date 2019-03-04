package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author: pxx
 * @Date: 2019/2/28 20:50
 * @Version 1.0
 */
public class CalleeWriteObserver extends BaseRegionObserver {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        //获取操作的目录表
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");

        //获取当前put数据的表
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();

        if (!targetTableName.equals(currentTableName)) {
            return;
        }
        //05_18549641558_2018-07-20 18:25:43_13980337439_1_0176
        String oriRowKey = Bytes.toString(put.getRow());

        String[] splitOriRowKey = oriRowKey.split("_");
        String caller = splitOriRowKey[1];
        String callee = splitOriRowKey[3];
        String buildTime = splitOriRowKey[2];

        //如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据)
        String flag = splitOriRowKey[4];
        String calleeflag = "0";
        if (flag.equals(calleeflag)) {
            return;
        }
        flag = calleeflag;

        String duration = splitOriRowKey[5];

        Integer regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        String regionCode = HBaseUtil.genRegionCode(caller,buildTime,regions);
        String calleeRowkey = HBaseUtil.genRowkey(regionCode, callee, buildTime, caller, flag, duration);
        String buildTimeTS = "";
        try {
            buildTimeTS  = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("callee"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("caller"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTimeReplace"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTS));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        Table table =  e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();

    }
}
