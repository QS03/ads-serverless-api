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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class DetailHeroHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        String caseNumber = "";
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            caseNumber = queryStringParameters.get("asap");
        }

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
                JSONArray details = runQuery(connection, caseNumber);
                statusCode = 200;
                retObject.put("data", details);
            } else {
                statusCode = 501;
                data.put("message", "Server error!");
                retObject.put("data", data);
            }
        } catch (JSONException e) {
            LOG.info("Error: {}", e);
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

    public JSONArray runQuery(Connection connection, String caseNumber) throws JSONException {

        String query = "--detail hero\n" +
                "SELECT DISTINCT\n" +
                "\t(a.CASE_NUMBER) as \"asap\", coalesce(b.remaining,0) as \"remaining\", c.completed as \"completed\", d.\"ASAP Total Cycle Time\" as \"totalCycleTime\", d.\"Avg ASAP Cycle Time - Days\" as \"avgCycleTime\", a.\"ASAP Status\" as \"isActive\", \"ASAP CREATED\" as \"dateStart\", e.dateend as \"dateEnd\"\n" +
                "FROM \"ADMIN\".\"sample_data_2\" a\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT a.CASE_NUMBER, count(a. \"Step Display Name\") AS remaining\n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\" a\n" +
                "\t\tLEFT JOIN (SELECT 'Completed Steps' AS metric, CASE_NUMBER, \"Step Display Name\", MAX(step_id) AS step_id\n" +
                "\t\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\t\tGROUP BY CASE_NUMBER, \"Step Display Name\", step_id) b ON a.STEP_ID = b.step_id\n" +
                "\t\t\tWHERE \"Date Out\" IS NULL\n" +
                "\t\t\tGROUP BY a.CASE_NUMBER) b ON a.CASE_NUMBER = b.case_number\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT a.CASE_NUMBER, count(a. \"Step Display Name\") AS completed\n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\" a\n" +
                "\t\tLEFT JOIN (SELECT 'Completed Steps' AS metric, CASE_NUMBER, \"Step Display Name\", MAX(step_id) AS step_id\n" +
                "\t\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\t\tGROUP BY CASE_NUMBER, \"Step Display Name\", step_id) b ON a.STEP_ID = b.step_id\n" +
                "\t\t\tWHERE \"Date Out\" IS NULL GROUP BY a.CASE_NUMBER) c ON a.CASE_NUMBER = c.case_number\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT CASE_NUMBER, AVG(\"Cycle Time - Days\") as \"Avg ASAP Cycle Time - Days\", AVG(\"ASAP Total Cycle Time\") as \"ASAP Total Cycle Time\" \n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\tGROUP BY case_number) d on a.CASE_NUMBER = d.case_number\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT a.CASE_NUMBER, max(a.\"Date Out\") AS dateend\n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\" a\n" +
                "\t\tLEFT JOIN (SELECT 'Completed Steps' AS metric, CASE_NUMBER, \"Step Display Name\", MAX(step_id) AS step_id\n" +
                "\t\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\t\tGROUP BY CASE_NUMBER, \"Step Display Name\", step_id) b ON a.STEP_ID = b.step_id\n" +
                "\t\t\tWHERE \"ASAP Status\" = 'Completed'\n" +
                "\t\t\tGROUP BY a.CASE_NUMBER) e ON a.CASE_NUMBER = e.case_number\n";
        if(!caseNumber.equals(""))query += "WHERE a.case_number = '" + caseNumber + "'";


        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray details = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while(rs.next()) {
                JSONObject item = new JSONObject();
                item.put("asap", rs.getString("asap"));
                item.put("remaining", rs.getInt("remaining"));
                item.put("completed", rs.getInt("completed"));
                item.put("totalCycleTime", rs.getFloat("totalCycleTime"));
                item.put("avgCycleTime", rs.getFloat("avgCycleTime"));
                item.put("isActive", rs.getString("isActive"));
                item.put("dateStart", rs.getString("dateStart"));
                item.put("dateEnd", rs.getString("dateEnd"));
                details.put(item);
            }
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        }
        return details;
    }
}
