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

public class SankeyDiagramHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

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
                        JSONObject arrayData = new JSONObject();
                        JSONArray nodeDataArray = getNodeDataArray(connection, startDate, endDate, organizations);
                        arrayData.put("nodeDataArray", nodeDataArray);
                        JSONArray linkDataArray = getLinkDataArray(connection, startDate, endDate, organizations);
                        arrayData.put("linkDataArray", linkDataArray);

                        retObject.put("data", arrayData);
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

    public JSONArray getNodeDataArray(Connection connection, String startDate, String endDate, JSONArray organizations) throws JSONException {

        String query = "SELECT\n" +
                "\ta. \"Step Display Name\" as \"key\",\n" +
                "\tCONCAT(a. \"Step Display Name\",CONCAT(' (', CONCAT(COUNT(DISTINCT (a.CASE_NUMBER)) ,')')))AS \"text\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = '3rd Level Approval (Manager)' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Review' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Approval' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Classification Review' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Issue Classification Advisory' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'DCA Finalizes Classification' THEN 1\n" +
                "\tEND AS \"standardMin\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = '3rd Level Approval (Manager)' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Review' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Approval' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Classification Review' THEN 5\n" +
                "\tWHEN a.\"Step Display Name\" = 'Issue Classification Advisory' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'DCA Finalizes Classification' THEN 3\n" +
                "\tEND AS \"standardMax\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = 'Submitted' then '#2a487b'\n" +
                "    WHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' then '#545E82'\n" +
                "    WHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' then '#898dadb'\n" +
                "    WHEN a.\"Step Display Name\" = 'Manpower Review' then '#B7B7C5'\n" +
                "    WHEN a.\"Step Display Name\" = 'Manpower Approval' then '#D6D6DC'\n" +
                "    WHEN a.\"Step Display Name\" = 'Classification Review' then '#D2D2D2'\n" +
                "    WHEN a.\"Step Display Name\" = 'Issue Classification Advisory' then '#f1f1f1'\n" +
                "    WHEN a.\"Step Display Name\" = 'DCA Finalizes Classification' then '#ffffff'\n" +
                "    END AS \"color\"\n" +
                "FROM \"ADMIN\".\"sample_data_2\" a\n" +
                "\tINNER JOIN (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM ( SELECT \"Step Display Name\", \"CASE_NUMBER\", max(step_id) AS step_id\n" +
                "\t\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\t\tWHERE \"Date In\" IS NOT NULL and \"Date Out\" is not null\n" +

                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "\t\tGROUP BY \"Step Display Name\", CASE_NUMBER)) b on a.step_id = b.step_id\n" +
                "GROUP BY a. \"Step Display Name\"\n" +
                "UNION \n" +
                "SELECT  \n" +
                "\tCONCAT(a.\"Step Display Name\",' In Progress') as \"key\",\n" +
                "\tCONCAT(a.\"Step Display Name\",CONCAT(' (',CONCAT(a.in_progress,')'))) AS \"text\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = '3rd Level Approval (Manager)' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Review' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Approval' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Classification Review' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Issue Classification Advisory' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'DCA Finalizes Classification' THEN 1\n" +
                "\tEND AS \"standardMin\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = '3rd Level Approval (Manager)' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Review' THEN 3\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Approval' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'Classification Review' THEN 5\n" +
                "\tWHEN a.\"Step Display Name\" = 'Issue Classification Advisory' THEN 1\n" +
                "\tWHEN a.\"Step Display Name\" = 'DCA Finalizes Classification' THEN 3\n" +
                "\tEND AS \"standardMax\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = 'Submitted' then '#2a487b'\n" +
                "    WHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' then '#545E82'\n" +
                "    WHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' then '#898dadb'\n" +
                "    WHEN a.\"Step Display Name\" = 'Manpower Review' then '#B7B7C5'\n" +
                "    WHEN a.\"Step Display Name\" = 'Manpower Approval' then '#D6D6DC'\n" +
                "    WHEN a.\"Step Display Name\" = 'Classification Review' then '#D2D2D2'\n" +
                "    WHEN a.\"Step Display Name\" = 'Issue Classification Advisory' then '#f1f1f1'\n" +
                "    WHEN a.\"Step Display Name\" = 'DCA Finalizes Classification' then '#ffffff'\n" +
                "    END AS \"color\"\n" +
                "\tFROM (SELECT \"Step Display Name\", COUNT(DISTINCT((\"CASE_NUMBER\"))) AS in_progress\n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\tWHERE \"Date In\" IS NOT NULL AND \"Date Out\" IS NULL\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }
                query += "\t\tGROUP BY \"Step Display Name\") a";

        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray resultArray = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("key", rs.getString("key"));
                item.put("text", rs.getString("text"));
                item.put("color", rs.getString("color"));
                resultArray.put(item);
            }
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        LOG.info("Counts: {}", resultArray.length());
        return resultArray;
    }

    public JSONArray getLinkDataArray(Connection connection, String startDate, String endDate, JSONArray organizations) throws JSONException {

        String query = "SELECT\n" +
                "\ta. \"Step Display Name\" as \"from\",\n" +
                "\tCASE WHEN a.\"Step Display Name\" = 'Submitted' THEN '2nd Level Review (Manager)'\n" +
                "\tWHEN a.\"Step Display Name\" = '2nd Level Review (Manager)' THEN '2nd Level Approval (Manager)'\n" +
                "\tWHEN a.\"Step Display Name\" = '2nd Level Approval (Manager)' THEN '3rd Level Approval (Manager)'\n" +
                "\tWHEN a.\"Step Display Name\" = '3rd Level Approval (Manager)' THEN 'Manpower Review'\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Review' THEN 'Manpower Approval'\n" +
                "\tWHEN a.\"Step Display Name\" = 'Manpower Approval' THEN 'Classification Review'\n" +
                "\tWHEN a.\"Step Display Name\" = 'Classification Review' THEN 'Issue Classification Advisory'\n" +
                "\tWHEN a.\"Step Display Name\" = 'Issue Classification Advisory' THEN 'DCA Finalizes Classification'\n" +
                "\tEND AS \"to\",\n" +
                "\tCOUNT(DISTINCT (a.CASE_NUMBER)) as \"width\"\n" +
                "FROM \"ADMIN\".\"sample_data_2\" a\n" +
                "\tINNER JOIN ( SELECT * FROM (\n" +
                "\t\tSELECT \"Step Display Name\", \"CASE_NUMBER\", max(step_id) AS step_id\n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\tWHERE \"Date In\" IS NOT NULL and \"Date Out\" is not null\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }

                query += "\t\tGROUP BY \"Step Display Name\", CASE_NUMBER)) b on a.step_id = b.step_id\n" +
                "GROUP BY a. \"Step Display Name\"\n" +
                "UNION \n" +
                "SELECT  \n" +
                "\ta.\"Step Display Name\" as \"from\",\n" +
                "\tCONCAT(a.\"Step Display Name\",' In Progress') \"to\",\n" +
                "\ta.in_progress \tas \"width\"\t\n" +
                "FROM (SELECT \"Step Display Name\", COUNT(DISTINCT((\"CASE_NUMBER\"))) AS in_progress\n" +
                "\t\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\t\tWHERE \"Date In\" IS NOT NULL AND \"Date Out\" IS NULL\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate);

                for (int i=0; i<organizations.length(); i++){
                    query += "\tand \"Org Code\" = '" + organizations.getString(i) + "'\n";
                }
                query += "\t\tGROUP BY \"Step Display Name\") a";


        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray resultArray = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("from", rs.getString("from"));
                item.put("to", rs.getString("to"));
                item.put("width", rs.getInt("width"));
                resultArray.put(item);
            }
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        LOG.info("Counts: {}", resultArray.length());
        return resultArray;
    }
}
