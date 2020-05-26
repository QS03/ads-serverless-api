// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.serverless;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	public Connection getConnection(DBCredentials dbCreds) {
		Connection connection = null;
		String url = "jdbc:oracle:thin:@//" + dbCreds.getDbHost() + ":" + dbCreds.getDbPort() + "/" + dbCreds.getDbName();
		try {
			connection = DriverManager.getConnection(url, dbCreds.getUserName(), dbCreds.getPassword());
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Could not get a connection to database.");
		}
		return connection;
	}
}
