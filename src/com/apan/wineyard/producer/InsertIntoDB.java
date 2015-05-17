package com.apan.wineyard.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;

public class InsertIntoDB implements Runnable{

	private File file;
	private BufferedReader bufferedReader;
	private DBCollection shoppingList;
	private ConcurrentHashMap<String, Integer> personPriorityMap = new ConcurrentHashMap<String,Integer>();
	static long id = 0;
	
	public InsertIntoDB(String fileName,DBCollection shoppingList){
		this.file = new File(fileName);
		try {
			FileReader reader = new FileReader(file);
			bufferedReader = new BufferedReader(reader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.shoppingList  = shoppingList;
	}
	
	@Override
	public void run(){
		String s = null;
		BulkWriteOperation bulk = shoppingList.initializeUnorderedBulkOperation();
		try {
			int buffered = 0;
			while((s = bufferedReader.readLine()) != null){
				StringTokenizer tokens = new StringTokenizer(s,"\t");
				if(tokens != null && tokens.countTokens() == 2){
					String person = tokens.nextToken().trim();
					String wineId = tokens.nextToken().trim();
					BasicDBObject dbObject = this.createDBObject(person,wineId);
					bulk.insert(dbObject);
					buffered++;
					if(buffered%100000 == 0){
						bulk.execute();
						buffered = 0;
						System.out.println("Inserted so far :: "+ shoppingList.getCount()+" records");
						bulk = shoppingList.initializeUnorderedBulkOperation();
					}
				}
			}
			bufferedReader.close();
			if(buffered > 0){
				bulk.execute();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
		
	BasicDBObject createDBObject(String personName,String wineId){
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("personName", personName);
		dbObject.put("wineId", wineId);

		return dbObject;
	}

	public ConcurrentHashMap<String, Integer> getPersonPriorityMap() {
		return personPriorityMap;
	}

	public BufferedReader getBufferedReader() {
		return bufferedReader;
	}
	
}
