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


public class DetailStepHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        String startDate = null;
        String endDate = null;
        String caseNumber = "";
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            startDate = queryStringParameters.get("start");
            endDate = queryStringParameters.get("end");
            caseNumber = queryStringParameters.get("casenumber");
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

        DBCredentials dbCreds = new DBCredentials();
        dbCreds.setDbHost("covid-oracle.cewagdn2zv2j.us-west-2.rds.amazonaws.com");
        dbCreds.setDbPort("1521");
        dbCreds.setUserName("admin");
        dbCreds.setPassword("8iEkGjQgFJzOblCihFaz");
        dbCreds.setDbName("orcl");

        DBConnection dbConnection = new DBConnection();
        Connection connection = dbConnection.getConnection(dbCreds);

        boolean isValidOrg = true;
        if(organizations == null){
            try {
                organizations = getOrganizations(connection);
            } catch (JSONException e) {
                e.printStackTrace();
            }
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
                try {
                    if (Optional.ofNullable(connection).isPresent()) {
                        statusCode = 200;
                        JSONArray detailSteps = getDetailSteps(connection, caseNumber);
                        JSONArray stepDurations = getStepDurations(connection, startDate, endDate, organizations);
                        data.put("detailSteps", detailSteps);
                        data.put("stepDurations", stepDurations);
                    } else {
                        statusCode = 501;
                        data.put("message", "Server error!");
                    }

                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            } else {
                statusCode = 400;
                try {
                    data.put("message", "Invalid date format");
                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            }
        }

        try {
            retObject.put("data", data);
        } catch (JSONException e) {
            e.printStackTrace();
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

    public JSONArray getStepDurations(Connection connection, String startDate, String endDate, JSONArray organizations) throws JSONException {

        String query = "SELECT\n" +
                "\t\"Step Display Name\" AS \"name\",\n" +
                "\tAVG(\"Cycle Time - Days\") AS \"averageDuration\",\n" +
                "\tMIN(\"Cycle Time - Days\") AS \"min\",\n" +
                "\tMAX(\"Cycle Time - Days\") AS \"max\",\n" +
                "\tCASE WHEN \"Step Display Name\" = 'Step Display Name 2' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 3' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 4' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 5' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 6' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 7' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 8' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 9' THEN 1\n" +
                "\tEND AS \"standardMin\",\n" +
                "\tCASE WHEN \"Step Display Name\" = 'Step Display Name 2' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 3' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 4' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 5' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 6' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 7' THEN 5\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 8' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 9' THEN 3\n" +
                "\tEND AS \"standardMax\"\n" +
                "FROM (\n" +
                "\tSELECT\n" +
                "\t\tCASE_NUMBER,\n" +
                "\t\t\"ASAP CREATED\",\n" +
                "\t\t\"Step Display Name\",\n" +
                "\t\t\"Date In\",\n" +
                "\t\t\"Date Out\",\n" +
                "\t\tCASE WHEN \"Date Out\" IS NULL AND \"Date In\" IS NOT NULL THEN \n" +
                "\t\ttrunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\", 2)\n" +
                "\t\tELSE CAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                "\t\tEND AS \"Cycle Time - Days\",\n" +
                "\t\tCASE WHEN \"Date Out\" IS NULL AND \"Date In\" IS NOT NULL THEN trunc((cast(CURRENT_TIMESTAMP AS date) - \"Date In\") * 24, 2)\n" +
                "\t\tELSE CAST(\"Cycle Time - Hours\" AS NUMBER)\n" +
                "\t\tEND AS \"Cycle Time - Hours\",\n" +
                "\t\t\"ASAP Status\"\n" +
                "\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                String.format("\tWHERE \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                query += "    AND (\n";

                for (int i=0; i<organizations.length(); i++){
                    if(i == 0){
                        query += "\t \"Org Code\" = '" + organizations.getString(i) + "'\n";
                    }
                    else {
                        query += "\tOR \"Org Code\" = '" + organizations.getString(i) + "'\n";
                    }
                }

                query += ")\n";

                query += ")\n" +
                "WHERE \"Step Display Name\" is not NULL\n" +
                "GROUP BY\n" +
                "\t\"Step Display Name\"";

        LOG.info("getDetailSteps query: {}", query);

        JSONArray stepDurations = new JSONArray();
        try {
            PreparedStatement prepStmt = connection.prepareStatement(query);
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("name", rs.getString("name"));
                item.put("averageDuration", rs.getFloat("averageDuration"));
                item.put("min", rs.getFloat("min"));
                item.put("max", rs.getFloat("max"));
                item.put("standardMin", rs.getFloat("standardMin"));
                item.put("standardMax", rs.getFloat("standardMax"));
                stepDurations.put(item);
            }
            LOG.info("Counts: {}", stepDurations.length());
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        return stepDurations;
    }

    JSONArray getDetailSteps(Connection connection, String caseNumber) {

        String query = "--detail step\n" +
                "SELECT case_number as \"asap\", \"Step Display Name\" as \"step\", sum(\"Cycle Time - Days\") as \"avgRoleTime\"\n" +
                "FROM \"ADMIN\".\"sample_data_2\"\n" +
                "WHERE \"Cycle Time - Days\" IS NOT NULL\n";
        if(!caseNumber.equals(""))query += "AND case_number = '"+ caseNumber + "'\n";
        query += "GROUP BY CASE_NUMBER, \"Step Display Name\"";

        LOG.info("getDetailSteps query: {}", query);


        JSONArray detailSteps = new JSONArray();
        try {
            PreparedStatement prepStmt = connection.prepareStatement(query);
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("asap", rs.getString("asap"));
                item.put("step", rs.getString("step"));
                item.put("avgRoleTime", rs.getFloat("avgRoleTime"));
                detailSteps.put(item);
            }
            LOG.info("Counts: {}", detailSteps.length());
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        return detailSteps;
    }

    public JSONArray getOrganizations(Connection connection) throws JSONException {

        String query = "SELECT DISTINCT (\"Org Code\")\n" +
                "FROM \"ADMIN\".\"sample_data_2\"\n" +
                "WHERE \"Org Code\" IS NOT NULL";


        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray orgs = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while(rs.next()) {
                JSONObject item = new JSONObject();
                orgs.put(rs.getString("Org Code"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return orgs;
    }
}
