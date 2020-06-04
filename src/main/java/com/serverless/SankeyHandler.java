package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;


public class SankeyHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        String startDate = null;
        String endDate = null;
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            startDate = queryStringParameters.get("start");
            endDate = queryStringParameters.get("end");
        }

        if (startDate == null) startDate = "1900-01-01";
        if (endDate == null) endDate = "2100-01-01";

        JSONArray organizations = null;
        try {
            if (input.get("body") != null){
                JSONObject body = new JSONObject((String) input.get("body"));
                organizations = (JSONArray) body.get("organizations");
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        boolean isValidOrg = true;
        if(organizations == null){
            organizations = new JSONArray();
        } else {
            for(int i=0;  i<organizations.length(); i++){
                try {
                    String org = organizations.getString(i);
                    if(!org.equals("Org 1") &&
                            !org.equals("Org 2") &&
                            !org.equals("Org 3") &&
                            !org.equals("Org 4") &&
                            !org.equals("Org 5") &&
                            !org.equals("Org 6")){
                        isValidOrg = false;
                        break;
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    isValidOrg = false;
                }
            }
        }
        LOG.info("organizations: {}", organizations);
        if(!isValidOrg){
            try {
                data.put("message", "Bad Request: Invalid Org Code");
                retObject.put("data", data);
            } catch (JSONException e) {
                LOG.info("Error: {}", e);
            }
        } else {
            if (!Validator.isValidateDate(startDate) || !Validator.isValidateDate(endDate)) {
                try {
                    data.put("message", "Invalid date format");
                    retObject.put("data", data);
                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            } else {
                DBCredentials dbCreds = new DBCredentials();
                dbCreds.setDbHost("covid-oracle.cewagdn2zv2j.us-west-2.rds.amazonaws.com");
                dbCreds.setDbPort("1521");
                dbCreds.setUserName("admin");
                dbCreds.setPassword("8iEkGjQgFJzOblCihFaz");
                dbCreds.setDbName("orcl");

                DBConnection dbConnection = new DBConnection();
                Connection connection = dbConnection.getConnection(dbCreds);

                try {
                    if (Optional.ofNullable(connection).isPresent()) {
                        statusCode = 200;
                        JSONArray sankeyData = getSankeyData(connection, startDate, endDate, organizations);
                        retObject.put("data", sankeyData);
                    } else {
                        statusCode = 501;
                        data.put("message", "Server error!");
                        retObject.put("data", data);
                    }

                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            }
        }

        return ApiGatewayResponse.builder()
                .setStatusCode(statusCode)
                .setRawBody(retObject.toString())
                .setHeaders(Collections.singletonMap("Content-Type", "application/json"))
                .setHeaders(Collections.singletonMap("Access-Control-Allow-Origin", "*"))
                .build();
    }

    public JSONArray getSankeyData(Connection connection, String startDate, String endDate, JSONArray organizations) throws JSONException {

        String query = "SELECT\n" +
                "\ta. \"Step Display Name\",\n" +
                "\tCOUNT(DISTINCT (a. CASE_NUMBER)) AS complete,\n" +
                "\tb.in_progress,\n" +
                "\tCASE WHEN a. \"Step Display Name\" = 'Step Display Name 2' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 3' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 4' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 5' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 6' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 7' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 8' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 9' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 10' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 12' THEN\n" +
                "\t\t1\n" +
                "\tEND AS \"standardMin\",\n" +
                "\tCASE WHEN a. \"Step Display Name\" = 'Step Display Name 2' THEN\n" +
                "\t\t3\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 3' THEN\n" +
                "\t\t3\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 4' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 5' THEN\n" +
                "\t\t3\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 6' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 7' THEN\n" +
                "\t\t3\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 8' THEN\n" +
                "\t\t3\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 9' THEN\n" +
                "\t\t3\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 10' THEN\n" +
                "\t\t1\n" +
                "\tWHEN a. \"Step Display Name\" = 'Step Display Name 12' THEN\n" +
                "\t\t3\n" +
                "\tEND AS \"standardMax\"\n" +
                "FROM\n" +
                "\t\"ADMIN\".\"sample_data_2\" a\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT\n" +
                "\t\t\t\"Step Display Name\",\n" +
                "\t\t\tCOUNT(DISTINCT ((CASE_NUMBER))) AS in_progress\n" +
                "\t\tFROM\n" +
                "\t\t\t\"ADMIN\".\"sample_data_2\"\n" +
                "\t\tWHERE\n" +
                "\t\t\t\"Date In\" IS NOT NULL\n" +
                "\t\t\tAND \"Date Out\" IS NULL\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

        query +="\t\tGROUP BY\n" +
                "\t\t\t\"Step Display Name\") b ON a. \"Step Display Name\" = b. \"Step Display Name\"\n" +
                "WHERE\n" +
                "\ta. \"Date In\" IS NOT NULL\n" +
                "\tAND a. \"Step Display Name\" IS NOT NULL\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

        query += "AND \"Org Code\" = 'Org 1'" +
                "GROUP BY\n" +
                "\ta. \"Step Display Name\",\n" +
                "\tb.in_progress";

        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray resultArray = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("Step Display Name", rs.getString("Step Display Name"));
                item.put("COMPLETE", rs.getInt("COMPLETE"));
                item.put("IN_PROGRESS", rs.getInt("IN_PROGRESS"));
                item.put("standardMin", rs.getInt("standardMin"));
                item.put("standardMax", rs.getInt("standardMax"));
                resultArray.put(item);
            }
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        LOG.info("Counts: {}", resultArray.length());
        return resultArray;
    }

}
