package com.searce.app;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions {
	
	@Description("Config Filepath")
	String getConfigKind();
	void setConfigKind(String value);
	
	@Description("Config Filepath")
	String getConfigKeyName();
	void setConfigKeyName(String value);
	
	@Description("Output Filename.")
	String getOutputFolder();
	void setOutputFolder(String value);
	
	@Description("Output Filename.")
	String getOutputFilepath();
	void setOutputFilepath(String value);
	  
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
	Long getCounter();
	void setCounter(Long value);
	
}