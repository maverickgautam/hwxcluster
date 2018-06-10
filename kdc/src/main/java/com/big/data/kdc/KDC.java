package com.big.data.kdc;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.Random;

public class KDC {


    private File kdcBaseDir;
    private MiniKdc miniKdc;
    private Random randomValue = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(KDC.class);


    // All you need to about KDC and its usage
    // https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/sections/ugi.html
    // Kerberos Delegtion https://www.cyberark.com/threat-research-blog/weakness-within-kerberos-delegation/
    public KDC() throws Exception {

        // create a random directory
        String randomDir = "/tmp/kdc/" + randomValue.nextInt();
        kdcBaseDir = new File(randomDir);
        kdcBaseDir.mkdirs();

        // created kdc conf file. which is krb5.congf in real hosts
        Properties kdcConf = MiniKdc.createConf();

        // start the mini KDC
        // KDC has AS (Authentication Server) + TGS (Ticket Granting Server)
        miniKdc = new MiniKdc(kdcConf, kdcBaseDir);
        miniKdc.start();
        LOGGER.info("Starting KDC");
    }

    public MiniKdc getKdcHandler() {
        return miniKdc;
    }

    public File getKdcBaseDir() {
        return kdcBaseDir;
    }


    public void stopKDC() {
        if (miniKdc != null) {
            miniKdc.stop();
        }
        LOGGER.info("KDC stopped");
        FileUtils.deleteQuietly(kdcBaseDir);
    }

    public static void main(String[] args) throws Exception {

        KDC miniKdc = null;

        try {


            // KDC started
            miniKdc = new KDC();
            MiniKdc kdcHandler = miniKdc.getKdcHandler();


            // Using UserGroupInformation as KDC client, as it has all the necessary code to interact with KDC
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);


            // get the user which has run the JVM
            String userName = UserGroupInformation.getLoginUser().getShortUserName();

            // create a file to  hold the password for the user
            File keytabFile = new File(miniKdc.getKdcBaseDir(), userName + ".keytab");

            // the principalName convention is different from SPN(Service Principal Name) and UPN (User Principal Name
            // The principalName is like a SPN here.
            String principalName = userName + "/localhost";

            // create userName and password in the KDC database.
            // Save the userName password in the keyTable file too
            kdcHandler.createPrincipal(keytabFile, principalName);


            UserGroupInformation.loginUserFromKeytab(principalName, keytabFile.getPath());


            // create a proxy user, with ugi of the login user.
            UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("daud", UserGroupInformation.getCurrentUser());


            // job run on behalf of the proxy user
            proxyUser.doAs(new PrivilegedAction<Token<?>[]>() {
                @Override
                public Token<?>[] run() {
                    try {
                        Credentials credentials = new Credentials();
                        FileSystem fs = FileSystem.get(conf);
                        return fs.addDelegationTokens(
                                UserGroupInformation.getLoginUser().getUserName(),
                                credentials);


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(conf.toString());
                    return null;
                }
            });

        } catch (Exception e) {
            throw e;
        } finally {
            // KDC stopped
            miniKdc.stopKDC();
        }


    }


}
