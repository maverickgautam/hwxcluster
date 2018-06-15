package com.big.data.validator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class WebLinkValidatorTest {

    private final Configuration conf = new Configuration();
    private static FileSystem fs;
    private static String baseDir;
    private static String outputDir;
    private static String inputDir;
    private static final String NEW_LINE_DELIMETER = "\n";

    @BeforeClass
    public static void startup() throws Exception {

        Configuration conf = new Configuration();
        //set the fs to file:/// which means the local fileSystem
        // change this to point to your cluster Namenode
        conf.set("fs.default.name", "file:///");

        fs = FileSystem.getLocal(conf);
        baseDir = "/tmp/spark/WordCountSpark/" + UUID.randomUUID().toString();
        inputDir = baseDir + "/INPUT/";
        outputDir = baseDir + "/OUTPUT";

        File tempFile = new File(inputDir + "input.txt");
        String content = "devExpress,IB,devexpress.com,Development,control,";

        //Write the data into the local filesystem
        FileUtils.writeStringToFile(tempFile, content, "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);

        String content1 = "Braintree,IB,braintreepayments.com,E-commerce,payments,";
        //Write the data into the local filesystem
        FileUtils.writeStringToFile(tempFile, content1, "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);


        // Bad URL for DRAGON
        String content3 = "Dragon,IB,mugambodragonballz.com,E-commerce,payments,";
        //Write the data into the local filesystem
        FileUtils.writeStringToFile(tempFile, content3, "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);


    }

    @AfterClass
    public static void cleanup() throws Exception {
        //Delete the local filesystem folder after the Job is done
        //fs.delete(new Path(baseDir), true);
    }

    @Test
    public void WordCount() throws Exception {

        // Any argument passed with -DKey=Value will be parsed by ToolRunner
        String[] args = new String[]{
                "-D" + WebLinkValidator.INPUT_PATH + "=" + inputDir,
                "-D" + WebLinkValidator.OUTPUT_PATH + "=" + outputDir,
                // if running on cluster set IS_RUN_LOCALLY fasle
                "-D" + WebLinkValidator.IS_RUN_LOCALLY + "=true",
                "-D" + WebLinkValidator.DEFAULT_FS + "=file:///",
                "-D" + WebLinkValidator.DELEIMETER + "=,",
                "-D" + WebLinkValidator.NUM_PARTITIONS + "=1"

        };

        // argument used while executing the command
        System.out.println(Arrays.toString(args));

        WebLinkValidator.main(args);


        //Read the data from the outputfile
        File outputFile = new File(outputDir + "/part-00000");
        String fileToString = FileUtils.readFileToString(outputFile, "UTF-8");
        Map<String, Integer> wordToCount = new HashMap<>();

        //4 lines in output file, with one word per line
        Arrays.stream(fileToString.split(NEW_LINE_DELIMETER)).forEach(e -> {
            String[] wordCount = e.split(",");
            wordToCount.put(wordCount[2], Integer.parseInt(wordCount[5]));
        });

        //4 words .
        Assert.assertEquals(3L, wordToCount.size());
        Assert.assertEquals(200, wordToCount.get("devexpress.com").intValue());
        Assert.assertEquals(200, wordToCount.get("braintreepayments.com").intValue());
        Assert.assertEquals(1001, wordToCount.get("mugambodragonballz.com").intValue());
    }

}
