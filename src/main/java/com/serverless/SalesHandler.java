package com.serverless;

import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;


public class SalesHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private final Logger LOG = LogManager.getLogger(this.getClass());

	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
		// LOG.info("received: {}", input);				

		DBCredentials dbCreds = new DBCredentials();
		dbCreds.setDbHost("covid-oracle.cewagdn2zv2j.us-west-2.rds.amazonaws.com");
		dbCreds.setDbPort("1521");
		dbCreds.setUserName("admin");
		dbCreds.setPassword("8iEkGjQgFJzOblCihFaz");
		dbCreds.setDbName("orcl");

		DBConnection dbConnection = new DBConnection();
		Connection connection = dbConnection.getConnection(dbCreds);		

		int statusCode = 200;
		JSONObject retObject = new JSONObject();
		String query = "SELECT YEAR_ID, sum(sales) FROM ADMIN.\"sales_data_sample\" GROUP BY YEAR_ID";
		
		try {
			if (Optional.ofNullable(connection).isPresent()) {
				JSONArray salesData = runQuery(connection, query);
				retObject.put("data", salesData);
			}
			else {
				statusCode = 501;
				retObject.put("message", "Server error!");
			}	
		} catch (JSONException e) {
			LOG.info("Error: {}", e);	
		}		

		Response responseBody = new Response(retObject, statusCode);
		return ApiGatewayResponse.builder()
			.setStatusCode(statusCode)
			.setRawBody(retObject.toString())
			.setHeaders(Collections.singletonMap("Content-Type", "application/json"))
			.setHeaders(Collections.singletonMap("Access-Control-Allow-Origin", "*"))
			.build();
	}

	/**
	 * This method will run sample query against Oracle Database.
	 *
	 * @param connection
	 * @param query
	 */
	public JSONArray runQuery(Connection connection, String query) {
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		JSONArray result = new JSONArray();
		try {
			prepStmt = connection.prepareStatement(query);
			rs = prepStmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();

			while (rs.next()) {
				int numColumns = rsmd.getColumnCount();
				JSONObject obj = new JSONObject();
				for (int i=1; i<=numColumns; i++) {
					String column_name = rsmd.getColumnName(i);
					obj.put(column_name, rs.getObject(column_name));
				}
				result.put(obj);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return result;
	}
}
