package com.searce.app;

import java.sql.ResultSet;

import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")
public class CustomRowMapper implements RowMapper<KV<String, String>> {
	private String primaryKeyColumn;
	
	public CustomRowMapper(String primaryKeyColumn) {
		this.primaryKeyColumn = primaryKeyColumn;
	}
	
	@Override
    public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
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
		return KV.of(output, (this.primaryKeyColumn != "" ? resultSet.getString(this.primaryKeyColumn) : "0"));
    }
}
