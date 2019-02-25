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
		
		options.setBucket(configEntity.getString("bucket"));
		options.setOutputFolder(configEntity.getString("outputFolder"));
		options.setStartingPoint(Long.valueOf(configEntity.getLong("startingPoint")));
		options.setCounter(Long.valueOf(configEntity.getLong("counter")));
		
		options.setDatabaseDriver(configEntity.getString("databaseDriver"));
		options.setDatabaseConnectionURL(configEntity.getString("databaseConnectionURL"));
		options.setDatabaseUsername(configEntity.getString("databaseUsername"));
		options.setDatabasePassword(configEntity.getString("databasePassword"));
		options.setTableName(configEntity.getString("tableName"));
		options.setTableSchema(configEntity.getString("tableSchema"));
		options.setTableType(configEntity.getString("tableType"));
		options.setPrimaryKeyColumn(configEntity.getString("primaryKeyColumn"));
		
		options.setBQDataset(configEntity.getString("BQDataset"));
		options.setBQTable(configEntity.getString("BQTable"));
		
		options.setJobName(options.getTableSchema().toLowerCase().replace("_", "-")+ "-" + options.getTableName().toLowerCase().replace("_", "-"));
		
		String query = "SELECT * FROM " + options.getTableSchema() + "." + options.getTableName(); // + " WHERE ROWNUM <= 1";
		if(!options.getPrimaryKeyColumn().isEmpty()) {
			String countQuery = "SELECT NUM_ROWS - " + options.getStartingPoint().toString() + " FROM ALL_TABLES " 
								+ "WHERE OWNER = '" + options.getTableSchema() + "' AND TABLE_NAME = '" + options.getTableName() + "'";
			query = query + " WHERE ROWNUM <= "
						+ "(" + countQuery + ")"
						+ "ORDER BY " + options.getPrimaryKeyColumn() + " DESC";
		}
		options.setDatabseQuery(query);
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy.MM.dd/HH/");
		String outputFilepath = options.getOutputFolder() + dtf.format(LocalDateTime.now()) + options.getTableSchema() + "." + options.getTableName() + "-" + options.getCounter().toString();
		options.setOutputFilepath(outputFilepath);
		
		String outputSchemaPath = options.getBucket() + "schemas/" + options.getOutputFolder() + options.getTableSchema() + "." + options.getTableName();
		options.setOutputSchemapath(outputSchemaPath);
	}
	
	@SuppressWarnings("serial")
	public static class ConfigUpdater extends SimpleFunction<Long, Long> {
		private String configKind;
		private String configKeyName;
		private String dataset;
		private String table;
		private Map<String, String> row;

		public ConfigUpdater(Options options) {
			this.configKind = options.getConfigKind();
			this.configKeyName = options.getConfigKeyName();
			this.dataset = options.getBQDataset();
			this.table = options.getBQTable();
			this.row = new HashMap<String, String>();
			this.row.put("filename", options.getOutputFilepath());
			this.row.put("LoadDateTime", DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss").format(LocalDateTime.now())); 
			this.row.put("IsIncremental", "No");
			this.row.put("IsBatch", "Yes");
			this.row.put("IsRealTime", "No");
			this.row.put("SensorName", "");
			this.row.put("SensorID", "");
			this.row.put("StartDate", DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss").format(LocalDateTime.now()));
		}
		@Override
		public Long apply(Long count) {
			if(count.equals(0)) {
				return count;
			}
			
			{
				// Updating Config at Datastore
				Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
				Key configKey = datastore.newKeyFactory().setKind(this.configKind)
									.newKey(this.configKeyName);
				Entity configEntity = datastore.get(configKey);
				
				if(configEntity.getString("tableType").equals("update"))	count = (long) 0;
				
				Entity updatedConfigEntity = Entity.newBuilder(configEntity)
						.set("counter", Long.sum(configEntity.getLong("counter"), (long)1))
						.set("startingPoint", Long.sum(configEntity.getLong("startingPoint"), count.longValue()))
						.build();
				
				datastore.put(updatedConfigEntity);
				
				if(configEntity.contains("metadata")) {
					for(String name: configEntity.getEntity("metadata").getNames()) {
						this.row.put(name, configEntity.getEntity("metadata").getString(name));
					}
					this.row.put("EndDate", DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss").format(LocalDateTime.now()));
				}
				
			}
			
			{
				// Adding Row at BigQuery
				BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
				TableId tableId = TableId.of(this.dataset, this.table);
				bigquery.insertAll(
						InsertAllRequest.newBuilder(tableId)
							.addRow(this.row.get("filename"), this.row)
							.build());

			}
			
			return count;
		}
	}
}


















