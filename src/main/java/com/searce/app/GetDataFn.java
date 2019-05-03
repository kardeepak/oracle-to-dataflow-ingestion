package com.searce.app;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@SuppressWarnings("serial")
class GetDataFn extends DoFn<String,KV<String,String>> {
	 @ProcessElement
	 public void process(ProcessContext c, PipelineOptions ops) {
		Options options = ops.as(Options.class);
		String url = options.getURL();
		options.setURL(url);
		options.setCounter(
				StaticValueProvider.of(Long.valueOf(100))
		);
		try {
			Client client = Client.create();
			WebResource webResource = client.resource(url);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
			LocalDate date = LocalDate.parse(options.getFromDate(), formatter);
			LocalDate toDate = LocalDate.parse(options.getToDate(), formatter);
			while(true) {
				JsonObject input = new JsonObject();
				input.addProperty("FromDate", date.format(formatter));
				input.addProperty("ToDate", date.format(formatter));
				input.addProperty("compressionType", "A");
				
				ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input.toString());
	
				if (response.getStatus() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "
					     + response.getStatus());
				}
				String output = response.getEntity(String.class);
			
				GsonBuilder gson_builder = new GsonBuilder();
				gson_builder.registerTypeAdapter(
				        JsonElement.class,
				        new JsonDeserializer<JsonElement>() {
							@Override
							public JsonElement deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
								return json;
							}
				        });
				Gson gson = gson_builder.create();
				JsonElement element = gson.fromJson(output, JsonElement.class);
				JsonArray array = element.getAsJsonArray();
				for(JsonElement elem: array) {
					JsonObject obj = elem.getAsJsonObject();
					String columns = "", data = "";
					for(Map.Entry<String, JsonElement> entry: obj.entrySet()) {
						columns += entry.getKey() + ",";
						data += obj.get(entry.getKey()).toString() + ",";
					}
					c.output(KV.of(columns, data));
				}
				if(date.equals(toDate)) break;
				date = date.plusDays(1);
				System.out.println("Done for " + date.format(formatter));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }
}
