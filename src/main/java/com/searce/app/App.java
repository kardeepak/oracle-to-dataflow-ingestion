package com.searce.app;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class App {
	
  @SuppressWarnings("serial")
  static void run(Options options) {
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, String>> rows = p.apply("Reading Database", 
    		JdbcIO.<KV<String, String>>read()
    			.withDataSourceConfiguration(
    					JdbcIO.DataSourceConfiguration
    						.create(options.getDatabaseDriver(), options.getDatabaseConnectionURL())
    						.withUsername(options.getDatabaseUsername())
    						.withPassword(options.getDatabasePassword())
    			)
    			.withQuery(options.getDatabseQuery())
    			.withRowMapper(new CustomRowMapper(options.getPrimaryKeyColumn()))
    			.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
    	);
    
    		
    rows.apply("Extract Data For File", ParDo.of(new DoFn<KV<String, String>, String>() {
    		@ProcessElement
    		public void processElement(ProcessContext c) {
    			c.output(c.element().getKey());
    		}
    	}))
    	.apply("Write File", TextIO.write().withoutSharding().to(options.getOutputFilepath()));
    
    rows.apply("Extracting Primary Key", ParDo.of(new DoFn<KV<String, String>, Long>() {
    	@ProcessElement
    	public void processElement(ProcessContext c) {
    		c.output(Long.parseLong(c.element().getValue()));
    	}
    }))
    	.apply("Finding Maximum Primary Key", Max.longsGlobally())
    	.apply("Updating Config", MapElements.via(new OptionsFactory.ConfigUpdater(options)));
    
    p.run();
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	OptionsFactory.configure(options);
    run(options);
  }

}