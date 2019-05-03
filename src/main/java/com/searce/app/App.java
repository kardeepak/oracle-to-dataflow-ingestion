package com.searce.app;

import java.util.ArrayList;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class App {
	static PCollection<KV<String, String>> getRows(Pipeline p, Options options) {
		ArrayList<String> LINES = new ArrayList<String>();
		LINES.add("Nothin");
		return p.apply("Nothin'", Create.of(LINES)).setCoder(StringUtf8Coder.of())
			.apply("Read Data From Web API", ParDo.of(new GetDataFn()));
	}
	
	@SuppressWarnings("serial")
	static void run(Options options) {
	    Pipeline p = Pipeline.create(options);
	    PCollection<KV<String, String>> rows;
	
	    if(options.getURL() == null || options.getURL().isEmpty()) {
	    	rows = p.apply("Reading Database", 
	    		JdbcIO.<KV<String, String>>read()
	    			.withDataSourceConfiguration(
	    					JdbcIO.DataSourceConfiguration
	    						.create(options.getDatabaseDriver(), options.getDatabaseConnectionURL())
	    						.withUsername(options.getDatabaseUsername())
	    						.withPassword(options.getDatabasePassword())
	    			)
	    			.withQuery(options.getDatabseQuery())
	    			.withRowMapper(new CustomRowMapper())
	    			.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
	    	);
	    }
	    else	rows = getRows(p, options);
	
	    rows.apply("Extract Data From Rows", ParDo.of(new DoFn<KV<String, String>, String>() {
	    	@ProcessElement
	    	public void process(ProcessContext c) {
	    		KV<String, String> elem = (KV<String, String>)c.element();
	    		c.output(elem.getValue());
	    	}
	    })).apply("Write File To GCS", TextIO.write().withoutSharding().to(options.getOutputFilepath()));
	    
	    rows.apply("Extract Columns Names From Rows", ParDo.of(new DoFn<KV<String, String>, String>() {
	    	@ProcessElement
	    	public void process(ProcessContext c) {
	    		KV<String, String> elem = (KV<String, String>)c.element();
	    		c.output(elem.getKey());
	    	}
	    })).apply("Combine All", new Distinct<String>())
	    .apply("Write to Schema File in GCS", TextIO.write().withoutSharding().to(options.getOutputSchemapath()));
	    
	    rows.apply("Count Number of Records Processed", Count.globally())
	    	.apply("Updating Config In Datastore & Writing Metadata To BigQuery", MapElements.via(new OptionsFactory.ConfigUpdater(options)));
	    
	    p.run();
	}

	public static void main(String[] args) {
	    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		OptionsFactory.configure(options);
	    run(options);
	}

}