package com.big.data.validator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;

import java.io.Closeable;
import java.io.IOException;


public class WebLinkValidator extends Configured implements Tool, Closeable {
    // The job extends Configured implements Tool for parsing argumenets .

    public static final String INPUT_PATH = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";
    public static final String DELEIMETER = "spark.delimeter.value";

    private SQLContext sqlContext;
    private JavaSparkContext javaSparkContext;


    // this function is to wrap us local run and global run

    protected <T> JavaSparkContext getJavaSparkContext(final boolean isRunLocal,
                                                       final String defaultFs,
                                                       final Class<T> tClass) {
        final SparkConf sparkConf = new SparkConf()
                //Set spark conf here , after one gets spark context you can set hadoop configuration for InputFormats
                .setAppName(tClass.getSimpleName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        if (isRunLocal) {
            // mention Instead of * the number of threads you want
            sparkConf.setMaster("local[*]");
        }

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        if (defaultFs != null) {
            sparkContext.hadoopConfiguration().set("fs.defaultFS", defaultFs);
        }

        return sparkContext;
    }


    // The actual logic which will be converted to distributed mappers

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        String inputPath = conf.get(INPUT_PATH);
        String outputPath = conf.get(OUTPUT_PATH);
        String delim = conf.get(DELEIMETER);


        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), WebLinkValidator.class);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(inputPath);

        // No Anynomous class has been used anywhere and hence, The outer class need not implement Serialzable
        stringJavaRDD
                .map(new ValidateWebLink(delim))
                .repartition(conf.getInt(conf.get(NUM_PARTITIONS), 1))
                .saveAsTextFile(outputPath);


        return 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WebLinkValidator(), args);
    }


    // This is a static member of the outer classs
    // Do remember all the lambda function are instantiated on driver, serialized and sent to driver.
    public static class ValidateWebLink implements Function<String, String> {


        private String delimeter;
        // The Aerospike client is not serializable and neither there is a need to instatiate on driver
        private transient CloseableHttpClient httpclient;


        public ValidateWebLink(String delimeter) {

            this.delimeter = delimeter;
            //Add Shutdown hook to close the client gracefully
            //This is the place where u can gracefully clean your Service resources as there is no cleanup() function in Spark Map
            //JVMShutdownHook jvmShutdownHook = new JVMShutdownHook();
            //Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
        }


        @Override
        public String call(String inputString) throws Exception {

            String[] arr = inputString.split(delimeter);


            // Intitialize on the first call
            if (httpclient == null) {
                httpclient = HttpClients.createDefault();
            }


            HttpGet httpGet = new HttpGet("http://" + arr[2]);

            int returncode = 1001;
            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                returncode = response.getStatusLine().getStatusCode();
            } catch (Exception e) {
                // if you want log here
            }

            return new String(inputString) + returncode;

        }

        //When JVM is going down close the client
        private class JVMShutdownHook extends Thread {
            @Override
            public void run() {
                System.out.println("JVM Shutdown Hook: Thread initiated , shutting down service gracefully");
                IOUtils.closeQuietly(httpclient);
            }
        }
    }

}

