package com.searce.app;

import java.sql.ResultSet;

import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;

@SuppressWarnings("serial")
public class CustomRowMapper implements RowMapper<String> {
	
	@Override
    public String mapRow(ResultSet resultSet) throws Exception {
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
		return output;
    }
}
