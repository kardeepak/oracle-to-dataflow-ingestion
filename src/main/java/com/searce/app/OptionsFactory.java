package com.searce.app;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
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
		options.setCounter(
				StaticValueProvider.of(Long.valueOf(configEntity.getLong("counter")))
		);
		
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
		
		options.setJobName(options.getTableName().toLowerCase().replace("_", "-"));

		String query = "SELECT * FROM " + options.getTableSchema() + "." + options.getTableName();
		if(!options.getPrimaryKeyColumn().isEmpty() && options.getTableType().equals("append")) {
			String countQuery = "SELECT COUNT(*) - " + options.getStartingPoint() + " FROM " + options.getTableSchema() + "." + options.getTableName();
			query = query + " WHERE ROWNUM < (" + countQuery + ") ORDER BY " + options.getPrimaryKeyColumn() + " DESC";
		}
		options.setDatabseQuery(query);
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy.MM.dd/HH/");
		String outputFilepath = options.getBucket() + options.getOutputFolder() + dtf.format(ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))) + options.getTableName() + "-" + options.getCounter().get().toString();
		options.setOutputFilepath(outputFilepath);
		
		String outputSchemaPath = options.getBucket() + "schemas/" + options.getOutputFolder() + options.getTableName();
		options.setOutputSchemapath(outputSchemaPath);
		
		if(configEntity.contains("url") && !configEntity.getString("url").isEmpty()) {
			options.setURL(configEntity.getString("url").strip());
			options.setFromDate(configEntity.getString("FromDate").concat(" 00:00:00"));
			options.setToDate(ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("MM/dd/yyyy")).concat(" 23:59:59"));
			options.setNewFromDate(ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("MM/dd/yyyy")));
		}
	}
	
	@SuppressWarnings("serial")
	public static class ConfigUpdater extends SimpleFunction<Long, Long> {
		private String configKind;
		private String configKeyName;
		private String dataset;
		private String table;
		private String fromDate;
		private Map<String, String> row;
		private ValueProvider<Long> counter;

		public ConfigUpdater(Options options) {
			this.configKind = options.getConfigKind();
			this.configKeyName = options.getConfigKeyName();
			this.dataset = options.getBQDataset();
			this.table = options.getBQTable();
			this.fromDate = options.getNewFromDate();
			this.counter = options.getCounter();
			this.row = new HashMap<String, String>();
			this.row.put("filename", options.getOutputFilepath());
			this.row.put("LoadDateTime", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm:ss"))); 
			this.row.put("IsIncremental", (options.getStartingPoint().equals(0))? "No" : "Yes");
			this.row.put("IsBatch", "Yes");
			this.row.put("IsRealTime", "No");
			this.row.put("SensorName", "");
			this.row.put("SensorID", "");
			this.row.put("StartDate", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm:ss")));
		}
		@Override
		public Long apply(Long count) {			
			{
				// Updating Config at Datastore
				Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
				Key configKey = datastore.newKeyFactory().setKind(this.configKind)
									.newKey(this.configKeyName);
				Entity configEntity = datastore.get(configKey);
				
				if(configEntity.getString("tableType").equals("update"))	count = (long) 0;
				
				System.out.println("XXXXXXXXXX" + this.counter.get().toString());
				
				Entity updatedConfigEntity = Entity.newBuilder(configEntity)
						.set("counter", Long.sum(configEntity.getLong("counter"), (long)1))
						.set("startingPoint", Long.sum(configEntity.getLong("startingPoint"), count.longValue()))
						.set("FromDate", this.fromDate)
						.set("running", false)
						.build();
				
				datastore.put(updatedConfigEntity);
				
				if(configEntity.contains("metadata")) {
					for(String name: configEntity.getEntity("metadata").getNames()) {
						this.row.put(name, configEntity.getEntity("metadata").getString(name));
					}
					this.row.put("EndDate", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm:ss")));
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


















