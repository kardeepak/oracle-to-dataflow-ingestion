package com.searce.app;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")
public class CustomRowMapper implements RowMapper<KV<String, String>> {
	
	@Override
	public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		String columns = rsmd.getColumnName(1);
		int c = 2;
		while(true) {
			try {
				columns = columns + ", " + rsmd.getColumnName(c);
				c++;
			} catch(Exception e) {
				break;
			}
		}
		
		String output = resultSet.getString(1);
		int i = 2;
		while(true) {
			try {
				output = output + ", " + resultSet.getString(i);
				i++;
			} catch(Exception e) {
				break;
			}
		}
		return KV.of(columns, output);
    }
}
