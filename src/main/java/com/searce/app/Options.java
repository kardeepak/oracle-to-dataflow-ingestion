package com.searce.app;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, GcpOptions {
	
	@Description("Config Kind")
	String getConfigKind();
	void setConfigKind(String value);
	
	@Description("Config Key Name")
	String getConfigKeyName();
	void setConfigKeyName(String value);
	
	@Description("Output Bucket")
	String getBucket();
	void setBucket(String value);
	
	@Description("Output Folder")
	String getOutputFolder();
	void setOutputFolder(String value);
	
	@Description("Output Filename")
	String getOutputFilepath();
	void setOutputFilepath(String value);
	
	@Description("Output Schema Filepath")
	String getOutputSchemapath();
	void setOutputSchemapath(String value);
	  
	@Description("Database Driver Class Name")
	String getDatabaseDriver();
	void setDatabaseDriver(String value);
	  
	@Description("Database Connection URL")
	String getDatabaseConnectionURL();
	void setDatabaseConnectionURL(String value);
	  
	@Description("Database Username")
	String getDatabaseUsername();
	void setDatabaseUsername(String value);
	  
	@Description("Database Password")
	String getDatabasePassword();
	void setDatabasePassword(String value);

	@Description("Table Name")
	String getTableSchema();
	void setTableSchema(String value);

	@Description("Table Name")
	String getTableName();
	void setTableName(String value);

	@Description("Table Type")
	String getTableType();
	void setTableType(String value);
	
	@Description("Primary Key of Table")
	String getPrimaryKeyColumn();
	void setPrimaryKeyColumn(String value);
	  
	@Description("Starting Point")
	Long getStartingPoint();
	void setStartingPoint(Long value);
	
	@Description("Database Query")
	String getDatabseQuery();
	void setDatabseQuery(String value);
	
	@Description("Source Counter")
	ValueProvider<Long> getCounter();
	void setCounter(ValueProvider<Long> value);
	
	@Description("Bigquery Dataset")
	String getBQDataset();
	void setBQDataset(String value);
	
	@Description("Bigquery Table")
	String getBQTable();
	void setBQTable(String value);
	
	@Description("Web API URL")
	String getURL();
	void setURL(String value);
	
	@Description("Start Date for Web API")
	String getFromDate();
	void setFromDate(String value);
	
	@Description("End Date for Web API")
	String getToDate();
	void setToDate(String value);
	
	String getNewFromDate();
	void setNewFromDate(String value);
}