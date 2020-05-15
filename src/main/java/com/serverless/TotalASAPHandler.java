package com.serverless;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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

public class TotalASAPHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private final Logger LOG = LogManager.getLogger(this.getClass());

	public static boolean validateDate(String strDate)
	{
		if (!strDate.trim().equals("")) {
			SimpleDateFormat sdfrmt = new SimpleDateFormat("yyyy-MM-dd");
			sdfrmt.setLenient(false);
			try {
				Date javaDate = sdfrmt.parse(strDate);
			} catch (ParseException e) {
				return false;
			}
		}
		return true;
	}

	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
		// LOG.info("input: {}", input);

		String startDate = "";
		String endDate = "";

		int statusCode = 200;
		JSONObject retObject = new JSONObject();
		JSONObject data = new JSONObject();

		Map<String, String> queryStringParameters = (Map<String, String>) input.get("queryStringParameters");
		if(queryStringParameters != null ){
			startDate = queryStringParameters.get("start");
			endDate = queryStringParameters.get("end");
		}


		if(validateDate(startDate) && validateDate(endDate)) {
			DBCredentials dbCreds = new DBCredentials();
			dbCreds.setDbHost("covid-oracle.cewagdn2zv2j.us-west-2.rds.amazonaws.com");
			dbCreds.setDbPort("1521");
			dbCreds.setUserName("admin");
			dbCreds.setPassword("8iEkGjQgFJzOblCihFaz");
			dbCreds.setDbName("orcl");

			DBConnection dbConnection = new DBConnection();
			Connection connection = dbConnection.getConnection(dbCreds);

			String query = "SELECT\n" +
					"\tcount(DISTINCT (CASE_NUMBER)) as \"Total ASAP Requests\"\n" +
					"FROM\n" +
					"\tADMIN. \"Adjusted_Data\"";

			if (startDate != "") {
				query += "\nWHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')";
			}
			if (endDate != "") {
				query += "\nAND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')";
			}
			LOG.info("query: {}", query);

			try {
				if (Optional.ofNullable(connection).isPresent()) {
					int orderCount = runQuery(connection, query);
					data.put("count", orderCount);
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

		try {
			retObject.put("data", data);
		} catch (JSONException e) {
			LOG.info("Error: {}", e);
		}

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
	public int runQuery(Connection connection, String query) {
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		int result = 0;

		try {
			prepStmt = connection.prepareStatement(query);
			rs = prepStmt.executeQuery();
			rs.next();
			result = rs.getInt("Total ASAP Requests");
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}
}
