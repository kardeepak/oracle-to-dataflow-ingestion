package com.searce.app;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;


public class App {
	
  static void run(Options options) {
    Pipeline p = Pipeline.create(options);
    PCollection<String> rows = p.apply("Reading Database", 
    		JdbcIO.<String>read()
    			.withDataSourceConfiguration(
    					JdbcIO.DataSourceConfiguration
    						.create(options.getDatabaseDriver(), options.getDatabaseConnectionURL())
    						.withUsername(options.getDatabaseUsername())
    						.withPassword(options.getDatabasePassword())
    			)
    			.withQuery(options.getDatabseQuery())
    			.withRowMapper(new CustomRowMapper())
    			.withCoder(StringUtf8Coder.of())
    	);
    		
    rows.apply("Write File To GCS", TextIO.write().withoutSharding().to(options.getOutputFilepath()));
    
    rows.apply("Count Number of Records Processed", Count.globally())
    	.apply("Updating Config In Datastore & Writing Metadata To GCS & BigQuery", MapElements.via(new OptionsFactory.ConfigUpdater(options)));
    
    p.run();
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	OptionsFactory.configure(options);
    run(options);
  }

}