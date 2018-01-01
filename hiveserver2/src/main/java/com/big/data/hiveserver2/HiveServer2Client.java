package com.big.data.hiveserver2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.sql.*;

public class HiveServer2Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServer2Client.class);
    private static String HIVESERVE2DRIVER = "org.apache.hive.jdbc.HiveDriver";


    public static void main(String[] args) throws SQLException, IOException {

        // set the configuration
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        // /etc/security/keytab/hive.service.keytab is from local machine, This is the user which is executing the command
        UserGroupInformation.loginUserFromKeytab("hive/cluster20182.field.hortonworks.com@FIELD.HORTONWORKS.COM", "/etc/security/keytab/hive.service.keytab");


        // load the driver
        try {
            Class.forName(HIVESERVE2DRIVER);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Driver not found");
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://cluster20181.field.hortonworks.com:2181,cluster20180.field.hortonworks.com:2181,cluster20182.field.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/cluster20182.field.hortonworks.com@FIELD.HORTONWORKS.COM");
        Statement stmt = con.createStatement();

        // Table Name
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table " + tableName);

        LOGGER.info("Table {} is dropped", tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");

        // show tables
        String sql = "show tables '" + tableName + "'";
        LOGGER.info("Running {} ", sql);

        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            LOGGER.info(" return from HiveServer {}", res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        LOGGER.info("DESCRIBE newwly created table sql command :  {}" + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.printf("HOOOO");
            LOGGER.info("Return from HiveServer {}", res.getString(1) + "\t" + res.getString(2));
        }

        // close the connection
        con.close();
    }

}
