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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HeroStatsHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

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
            if (Validator.isValidateDate(startDate) && Validator.isValidateDate(endDate)) {
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
                        data = runQuery(connection, startDate, endDate, organizations);
                        statusCode = 200;
                        retObject.put("data", data);
                    } else {
                        statusCode = 501;
                        data.put("message", "Server error!");
                        retObject.put("data", data);
                    }
                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            } else {
                statusCode = 400;
                try {
                    data.put("message", "Invalid date format");
                    retObject.put("data", data);
                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            }
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Headers", "Content-Type, Origin, Access-Control-Allow-Headers");

        return ApiGatewayResponse.builder()
                .setStatusCode(statusCode)
                .setRawBody(retObject.toString())
                .setHeaders(headers)
                .build();
    }

    public JSONObject runQuery(Connection connection, String startDate, String endDate, JSONArray organizations) throws JSONException {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONObject result = new JSONObject();

        String query = "SELECT\n" +
                "\t'Total ASAP Requests' as metric,\n" +
                "\tcount(DISTINCT (CASE_NUMBER)) as count\n" +
                "FROM\n" +
                "\t\"ADMIN\".\"sample_data_2\"\t\n" +
                String.format("\tWHERE \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "\tAND \"Org Code\" = 'Org 1'\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "\t'Active ASAP Requests' as metric,\n" +
                "\tcount(DISTINCT (CASE_NUMBER)) as count\n" +
                "FROM\n" +
                "\t\"ADMIN\".\"sample_data_2\"\n" +
                "WHERE\n" +
                "\t\"ASAP Status\" = 'Active'\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "UNION ALL \n" +
                "SELECT\n" +
                "\t'Completed ASAP Requests' as metric,\n" +
                "\tcount(DISTINCT (CASE_NUMBER)) as count\n" +
                "FROM\n" +
                "\t\"ADMIN\".\"sample_data_2\"\n" +
                "WHERE\n" +
                "\t\"ASAP Status\" = 'Completed'\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "UNION ALL \n" +
                "SELECT\n" +
                "\t'Average ASAP Cycle Time - Days' as metric,\n" +
                "\tAVG(CAST(\"ASAP Total Cycle Time\" as FLOAT)) AS count\n" +
                "FROM\n" +
                "\t(SELECT\n" +
                "\t\tMAX(CAST(\"ASAP Total Cycle Time\" as FLOAT)) AS \"ASAP Total Cycle Time\",\n" +
                "\t\tCASE_NUMBER\n" +
                "\tFROM\n" +
                "\t\t\"ADMIN\".\"sample_data_2\"\n" +
                String.format("\tWHERE \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "\tGROUP BY\n" +
                "\t\tCASE_NUMBER)\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "\t'Fastest ASAP Total Cycle Time - Days' as metric,\n" +
                "\tMIN(CAST(\"ASAP Total Cycle Time\" as FLOAT)) AS count\n" +
                "FROM\n" +
                "\t(SELECT\n" +
                "\t\tMAX(CAST(\"ASAP Total Cycle Time\" as FLOAT)) AS \"ASAP Total Cycle Time\",\n" +
                "\t\tCASE_NUMBER,\n" +
                "\t\t\"ASAP Status\"\n" +
                "\tFROM\n" +
                "\t\t\"ADMIN\".\"sample_data_2\"\n" +
                String.format("\tWHERE \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "\tGROUP BY\n" +
                "\t\tCASE_NUMBER,\n" +
                "\t\t\"ASAP Status\")\n" +
                "WHERE\n" +
                "\t\"ASAP Status\" = 'Completed'\n" +
                "\tAND CAST(\"ASAP Total Cycle Time\" as FLOAT) > 0";

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while(rs.next()) {
                String key = rs.getString("metric");
                if(key.equals("Fastest ASAP Total Cycle Time") || key.equals("Average ASAP Cycle Time"))
                    result.put(rs.getString("metric"), rs.getFloat("count"));
                else
                    result.put(rs.getString("metric"), rs.getInt("count"));
            }

        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        }

        return result;
    }
}
