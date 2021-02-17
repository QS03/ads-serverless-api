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


public class AsapListHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();


        DBCredentials dbCreds = new DBCredentials();
        dbCreds.setDbHost("covid-oracle.xxxxxxx.us-west-2.rds.amazonaws.com");
        dbCreds.setDbPort("1521");
        dbCreds.setUserName("admin");
        dbCreds.setPassword("xxxxxxx");
        dbCreds.setDbName("orcl");

        DBConnection dbConnection = new DBConnection();
        Connection connection = dbConnection.getConnection(dbCreds);

        try {
            if (Optional.ofNullable(connection).isPresent()) {
                JSONArray details = runQuery(connection);
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

        String response = "";
        try {
            response = retObject.toString(4);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return ApiGatewayResponse.builder()
                .setStatusCode(statusCode)
                .setRawBody(response)
                .setHeaders(headers)
                .build();
    }

    public JSONArray runQuery(Connection connection) throws JSONException {

        String query = "WITH completed_steps AS (\n" +
                "\tSELECT case_number, \"ASAP CREATED\", count(DISTINCT (\"Step Display Name\")) AS completedsteps\n" +
                "\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\tWHERE \"Date Out\" IS NOT NULL\n" +
                "\tGROUP BY case_number, \"ASAP CREATED\"\n" +
                "),\n" +
                "step_times AS(\n" +
                "\tSELECT case_number, \"Step Display Name\", sum(\"Cycle Time - Days\") AS steptime\n" +
                "\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\tGROUP BY case_number, \"Step Display Name\"\n" +
                "),\n" +
                "step_standards as (\n" +
                "\tSELECT case_number, \"Step Display Name\", steptime,\n" +
                "\t\tCASE WHEN \"Step Display Name\" = 'Step Display Name 1' and steptime = 0 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = '2nd Level Review (Manager)' and steptime <= 3 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = '2nd Level Approval (Manager)' and steptime <= 3 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = '3rd Level Approval (Manager)' and steptime <= 3 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = 'Manpower Review' and steptime <= 3 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = 'Manpower Approval' and steptime <= 1 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = 'Classification Review' and steptime <= 5 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = 'Issue Classification Advisory' and steptime <= 1 then 1\n" +
                "\t\tWHEN \"Step Display Name\" = 'DCA Finalizes Classification' and steptime < 3 then 1\n" +
                "\t\tELSE 0\n" +
                "\t\tEND AS withinstandards\n" +
                "\tFROM step_times),\n" +
                "summary as (\n" +
                "\tSELECT a.case_number, sum(withinstandards) as ontimesteps, b.completedsteps, b.\"ASAP CREATED\"\n" +
                "\tFROM step_standards a \n" +
                "\tLEFT JOIN completed_steps b ON a.case_number = b.case_number\n" +
                "\tgroup by a.case_number, b.completedsteps, b.\"ASAP CREATED\")\n" +
                "SELECT\n" +
                "\tcase_number \"asap\",\n" +
                "\tontimesteps / completedsteps as \"percentCompletedWithinTime\",\n" +
                "\t\"ASAP CREATED\" \"createdAt\"\n" +
                "FROM\n" +
                "\tsummary";


        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray asapList = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while(rs.next()) {
                JSONObject item = new JSONObject();
                item.put("asap", rs.getString("asap"));
                item.put("percentCompletedWithinTime", rs.getFloat("percentCompletedWithinTime"));
                item.put("createdAt", rs.getString("createdAt"));
                asapList.put(item);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return asapList;
    }
}
