package com.searce.app;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.SimpleFunction;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;


public class OptionsFactory {
	
	public static void configure(Options options) {
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		Key configKey = datastore.newKeyFactory().setKind(options.getConfigKind())
							.newKey(options.getConfigKeyName());
		Entity configEntity = datastore.get(configKey);

		options.setOutputFolder((configEntity.getString("outputFolder")));
		options.setStartingPoint((Long.valueOf(configEntity.getLong("startingPoint"))));
		options.setCounter((Long.valueOf(configEntity.getLong("counter"))));
		
		options.setDatabaseDriver((configEntity.getString("databaseDriver")));
		options.setDatabaseConnectionURL((configEntity.getString("databaseConnectionURL")));
		options.setDatabaseUsername((configEntity.getString("databaseUsername")));
		options.setDatabasePassword((configEntity.getString("databasePassword")));
		options.setTableName((configEntity.getString("tableName")));
		options.setTableType((configEntity.getString("tableType")));
		options.setPrimaryKeyColumn((configEntity.getString("primaryKeyColumn")));
		
		options.setBQDataset(configEntity.getString("BQDataset"));
		options.setBQTable(configEntity.getString("BQTable"));
		
		String query = "SELECT * FROM " + options.getTableName();
		// if(!options.getPrimaryKeyColumn().isEmpty()) query = query + " WHERE " + options.getPrimaryKeyColumn() + " > " + options.getStartingPoint();
		options.setDatabseQuery((query));
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy.MM.dd/HH/");
		String outputFilepath = options.getOutputFolder() + dtf.format(LocalDateTime.now()) + options.getTableName() + "-" + options.getCounter();
		options.setOutputFilepath((outputFilepath));
	}
	
	@SuppressWarnings("serial")
	public static class ConfigUpdater extends SimpleFunction<Long, Long> {
		private String configKind;
		private String configKeyName;
		private String dataset;
		private String table;
		private Map<String, Object> row;

		public ConfigUpdater(Options options) {
			this.configKind = options.getConfigKind();
			this.configKeyName = options.getConfigKeyName();
			this.dataset = options.getBQDataset();
			this.table = options.getBQTable();
			this.row = new HashMap<String, Object>();
			this.row.put("filename", options.getOutputFilepath());
		}
		@Override
		public Long apply(Long startingPoint) {
			if(startingPoint.equals(Long.MIN_VALUE))	return startingPoint;
			{
				// Updating Config at Datastore
				Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
				Key configKey = datastore.newKeyFactory().setKind(this.configKind)
									.newKey(this.configKeyName);
				Entity configEntity = datastore.get(configKey);
				
				if(configEntity.getString("tableType").equals("update"))	startingPoint = (long) 0;
				
				Entity updatedConfigEntity = Entity.newBuilder(configEntity)
						.set("counter", configEntity.getLong("counter") + 1)
						.set("startingPoint", startingPoint)
						.build();
				
				datastore.put(updatedConfigEntity);
			}
			
			{
				// Adding Row at BigQuery
				BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
				TableId tableId = TableId.of(this.dataset, this.table);
				bigquery.insertAll(
						InsertAllRequest.newBuilder(tableId)
							.addRow("test", this.row)
							.build());

			}
			
			return startingPoint;
		}
	}
}


















