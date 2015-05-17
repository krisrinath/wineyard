package com.apan.wineyard.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class PrintOutput {
	private DBCollection shoppingList;
	private File file;
	private PrintWriter printWriter;
	private Cursor cursor;

	public PrintOutput(DBCollection shoppingList,String fileName){
		this.shoppingList = shoppingList;
		file = new File(fileName);
		try {
			if(!file.exists())
				file.createNewFile();
			
			printWriter = new PrintWriter(file);
			printWriter.write("");
			printWriter.println(shoppingList.getCount());
			printWriter.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		cursor = this.getAggregateCursor();
	}

	public void printOutput(){
		while(cursor.hasNext()){
			DBObject object = cursor.next();
			String personName = (String) object.get("personName");
			String wineId = (String) object.get("wineId");
			printWriter.println(personName+"\t"+wineId);
		}
		printWriter.flush();
		printWriter.close();
		printWriter.close();
	}

	private Cursor getAggregateCursor(){
		List<DBObject> conditions = new ArrayList<DBObject>();
		BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("personName",1));
		conditions.add(sort);

		AggregationOptions aggregationOptions = AggregationOptions.builder()
				.batchSize(100)
				.outputMode(AggregationOptions.OutputMode.CURSOR)
				.allowDiskUse(true)
				.build();

		return shoppingList.aggregate(conditions,aggregationOptions);
	}
	
}
