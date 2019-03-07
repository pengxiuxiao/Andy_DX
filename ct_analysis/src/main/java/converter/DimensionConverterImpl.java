package converter;

import kv.base.BaseDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.JDBCInstance;
import utils.JDBCUtils;
import utils.LURCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 1、根据传入的维度数据，得到该数据在表中的对应的主键ID
 *  做内存缓存 LURCatch
 *  分支
 *  --缓存中有数据，直接返回ID
 *  --缓存中没有数据 查询mysql
 *   分支：
 *   mysql中有该条数据 直接返回ID 将本次读取到的ID缓存到内存中
 *   mysql中没有数据 插入该条数据 再次反查3该数据 得到ID并返回 缓存到内存中
 *
 * @Author: pxx
 * @Date: 2019/3/6 21:08
 * @Version 1.0
 */
public class DimensionConverterImpl implements DimensionConverter {
    /**
     *Logger  打印该类的日志,取代resources里的log4j.properties
     */
    private static final Logger logger = LoggerFactory.getLogger(DimensionConverterImpl.class);

    //对象线程化 用于每个线程管理自己的jdbc连接器
    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    private LURCache lruCache = new LURCache(3000);

    public DimensionConverterImpl() {
        //jvm关闭时 释放资源
        Runtime.getRuntime().addShutdownHook(new Thread( () -> JDBCUtils.close(threadLocalConnection.get(), null, null)));
    }

    @Override
    public int getDimensionID(BaseDimension dimension) {
        //1、根据传入的维度对象获取相应的主键ID，先从LRUCache中获取
        //时间维度： date_dimension_year_month_day, 10
        //联系人维度： contact_dimension_telephone_name(到了电话号码就不会重复了), 12
        String cacheKey = genCacheKey(dimension);

        //尝试获取缓存的ID
        if (lruCache.containsKey(cacheKey)) {
            return lruCache.get(cacheKey);
        }
        //没有得到缓存ID 序执行select操作
        //SQLS包含了1组sql语句 查询和插入
        String[] sqls = null;
        if (dimension instanceof DateDimension) {
            sqls = getDateDimensionSQL();
        } else if (dimension instanceof ContactDimension) {
            sqls = getContactDimensionSQL();
        } else {
            throw new RuntimeException("没有匹配到对应维度信息.");
        }

        //准备对Mysql表进行操作 先查询 有可能在插入
        Connection connection = this.getConnect();
        int id = -1;
        synchronized (this) {
            id = execSQL(connection, sqls, dimension);
        }
        //将刚刚查询到的ID加入到缓存中
        lruCache.put(cacheKey,id);
        return id;
    }

    private int execSQL(Connection connection, String[] sqls, BaseDimension dimension) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //1
            //查询的preparedStatement
            preparedStatement = connection.prepareStatement(sqls[0]);
            //根据不同的维度 封装不同的sql语句
            setArguments(preparedStatement, dimension);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int result = resultSet.getInt(1);
                //释放资源
                JDBCUtils.close(null,preparedStatement,resultSet);
                return result;
            }
            //释放资源
            JDBCUtils.close(null,preparedStatement,resultSet);

            //2
            //执行插入 封装插入的SQL语句
            preparedStatement = connection.prepareStatement(sqls[1]);
            setArguments(preparedStatement, dimension);
            //执行插入
            preparedStatement.executeUpdate();
            //释放资源
            JDBCUtils.close(null,preparedStatement,resultSet);

            //3
            //查询的 preparedStatement
            preparedStatement = connection.prepareStatement(sqls[0]);
            //根据不同的维度 封装不同的SQL语句
            setArguments(preparedStatement, dimension);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //释放资源
            JDBCUtils.close(null,preparedStatement,resultSet);
        }
        return -1;
    }

    private void setArguments(PreparedStatement preparedStatement, BaseDimension dimension) {
        int i = 0;
        try {
            if (dimension instanceof DateDimension) {
                //可以优化
                DateDimension dateDimension = (DateDimension) dimension;
                preparedStatement.setString(++i, dateDimension.getYear());
                preparedStatement.setString(++i, dateDimension.getMonth());
                preparedStatement.setString(++i, dateDimension.getDay());
            } else if (dimension instanceof ContactDimension) {
                ContactDimension contactDimension = (ContactDimension) dimension;
                preparedStatement.setString(++i, contactDimension.getTelephone());
                preparedStatement.setString(++i, contactDimension.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 得到当前线程维护的connection对象
     * @return
     */
    private Connection getConnect() {
        Connection connection = null;
        try {
            connection = threadLocalConnection.get();
            if (connection == null || connection.isClosed()) {
                connection = JDBCInstance.gentInstance();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 返回联系人表的查询和插入语句
     * @return
     */
    private String[] getContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 返回时间表的查询和插入语句
     * @return 
     */
    private String[] getDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }


    /**
     * 根据维度信息得到维度对应的缓存建
     * @param dimension
     * @return
     */
    private String genCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if (dimension instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) dimension;
            sb.append("date_dimension")
                    .append(dateDimension.getYear())
                    .append(dateDimension.getMonth())
                    .append(dateDimension.getDay());
        } else if (dimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) dimension;
            sb.append("contact_dimension").append(contactDimension.getTelephone());
        }
        return sb.toString();
    }
}
