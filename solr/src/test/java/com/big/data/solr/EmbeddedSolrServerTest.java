package com.big.data.solr;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;


public class EmbeddedSolrServerTest {

    static final String CONFIGSET_DIR = "src/test/resources/configsets";

    static SolrClient solrClient;

    @BeforeClass
    public static void setupClass() {
        try {
            String targetLocation = EmbeddedSolrServerTest.class
                    .getProtectionDomain().getCodeSource().getLocation().getFile() + "/..";

            String solrHome = targetLocation + "/solr";

            solrClient = EmbeddedSolrServerUnit.create(solrHome, CONFIGSET_DIR, "exampleCollection");

            // create some test documents
            SolrInputDocument doc1 = new SolrInputDocument();
            doc1.addField("id", "1");

            SolrInputDocument doc2 = new SolrInputDocument();
            doc2.addField("id", "2");

            SolrInputDocument doc3 = new SolrInputDocument();
            doc3.addField("id", "3");

            SolrInputDocument doc4 = new SolrInputDocument();
            doc4.addField("id", "4");

            SolrInputDocument doc5 = new SolrInputDocument();
            doc5.addField("id", "5");

            // add the test data to the index
            solrClient.add(Arrays.asList(doc1, doc2, doc3, doc4, doc5));
            solrClient.commit();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @AfterClass
    public static void teardownClass() {
        try {
            solrClient.close();
        } catch (Exception e) {
        }
    }

    @Test
    public void testEmbeddedSolrServerFactory() throws IOException, SolrServerException {
        SolrQuery solrQuery = new SolrQuery("*:*");
        QueryResponse response = solrClient.query(solrQuery);
        Assert.assertNotNull(response);

        SolrDocumentList solrDocuments = response.getResults();
        Assert.assertNotNull(solrDocuments);
        Assert.assertEquals(5, solrDocuments.getNumFound());
    }

}


