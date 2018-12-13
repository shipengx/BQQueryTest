package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.UUID;

public class BQQueryTest {

    public static void main(String[] args) throws Exception {
        String jsonKeyFile = "/opt/opinmind/conf/credentials/adara-machinelearning-371a3b1781df.json";
        GoogleCredentials credentials;
        File credentialsPath = new File(jsonKeyFile);
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }

        String query = " SELECT cookie_id, id_value\n" +
                "FROM `adara-data-master.opinmind_prod.user_id_map`\n" +
                "WHERE id_type = 7\n" +
                "\tAND date(pixel_ts) > DATE_SUB(current_date, INTERVAL 30 DAY)\n" +
                "    AND id_value IN\n" +
                "         ( SELECT id_value\n" +
                "          FROM `adara-data-master.opinmind_prod.user_id_map`\n" +
                "          WHERE id_type = 7\n" +
                "              AND date(pixel_ts) > DATE_SUB(current_date, INTERVAL 30 DAY)\n" +
                "         GROUP BY 1\n" +
                "         HAVING count(distinct cookie_id) > 1\n" +
                "         AND count(distinct cookie_id) < 20 \n" +
                "         )\n" +
                "GROUP BY 1, 2; ";


        BigQuery bq = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("adara-machinelearning").build().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        query)
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        // [END bigquery_simple_app_query]

        // [START bigquery_simple_app_print]
        // Get the results.
        QueryResponse response = bq.getQueryResults(jobId);

        TableResult tableResult = queryJob.getQueryResults();

        // Print the results.
        int count = 0;
        for (FieldValueList row : tableResult.iterateAll()) {
            long cookieId = row.get("cookie_id").getLongValue();
            String deviceId = row.get("id_value").getStringValue();
            System.out.println(cookieId + "," + deviceId);

            count++;
            System.out.printf("\n");
        }
        System.out.println("the number of rows is : " + count);
    }

}
