package com.apan.wineyard.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.types.ObjectId;

import com.apan.wineyard.beans.PersonDuplicate;
import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PopulateWineRemovableIds implements Runnable{
	DBCollection shoppingList;
	DBCollection removableIds;
	DBCollection wineweight;
	DBCollection personPriority;
	Queue<String> wineIds = new ConcurrentLinkedQueue<String>();
	PopulateUtil populateUtil = new PopulateUtil();
	long deleted = 0;
	
	public PopulateWineRemovableIds(DBCollection shoppingList,DBCollection removableIds,DBCollection wineweight){
		this.shoppingList = shoppingList;
		this.removableIds = removableIds;
		this.wineweight = wineweight;
		this.populateWineIdsAndWtTable();
	}

	private void populateWineIdsAndWtTable(){
		Cursor wineIdsCursor = populateUtil.groupByField("$wineId",shoppingList,100000);

		BulkWriteOperation bulk = wineweight.initializeUnorderedBulkOperation();
		int cnt = 0,bufferCnt = 0;

		while(wineIdsCursor.hasNext()){
			DBObject object = wineIdsCursor.next();
			
			Integer count = (Integer) object.get("count");
			String wineId = (String) object.get("_id");
			
			if(count > 1){
				wineIds.add(wineId);
			}
			
			createWineWeightDBObject(wineId,count,bulk);
			
			cnt++;bufferCnt++;
			
			if(cnt%10000 == 0){
				System.out.println("wineIds count processed "+cnt+ " :: "+ wineIds.size()+" wineIds are duplicate");
			}
			
			if(bufferCnt > 0 && bufferCnt%100000 == 0){
				bulk.execute();
				bufferCnt = 0;
				System.out.println("Inserted into wineWeight :: "+ wineweight.getCount()+" records");
				bulk = wineweight.initializeUnorderedBulkOperation();
			}
		}
		
		if(bufferCnt > 0){
			bulk.execute();
			System.out.println("Inserted into wineWeight :: "+ wineweight.getCount()+" records");
		}
		wineweight.createIndex(new BasicDBObject("wineId", 1),"wineWeightWineIdIndex");
		wineIdsCursor.close();
	}
	
	void createWineWeightDBObject(String wineId,int weight,BulkWriteOperation bulk){
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("wineId", wineId);
		dbObject.put("wineWeight", weight);
		bulk.insert(dbObject);
	}
	
	@Override
	public void run(){
		removeWineIdDuplicate();
//		removePersonMoreThan3WineBottles();
	}
	
	void removeWineIdDuplicate(){
		String windId = null;
		int bulkCnt = 0;
		BulkWriteOperation bulk = this.removableIds.initializeUnorderedBulkOperation();
		while(wineIds.size() > 0 && (windId=wineIds.remove()) != null){
			int inserts = this.addRemovableObjects(windId,shoppingList,bulk);
			bulkCnt = bulkCnt + inserts;
			if(bulkCnt > 0 && bulkCnt >= 10000 && (bulkCnt%10000 <= inserts)){
				System.out.println(bulkCnt+" records to be inserted into removableIds");
				bulk.execute();
				System.out.println(removableIds.count()+" removable Ids found so far");
				bulkCnt = 0;
				bulk = this.removableIds.initializeUnorderedBulkOperation();
			}
		}

		if(bulkCnt > 0){
			bulk.execute();
		}
		System.out.println(removableIds.count()+" removable Ids found so far");
	}
	
	int addRemovableObjects(String wineId,DBCollection shoppingList,BulkWriteOperation bulk){
		Cursor wineIdcursor = shoppingList.find(new BasicDBObject("wineId",new BasicDBObject("$in",new String[]{wineId})));
		int toBeRemoved = 0;
		HashMap<String,ObjectId> personNameIdMap = new HashMap<String,ObjectId>();
		int leastCnt = 0;
		String mostWtPerson = null;
		while(wineIdcursor.hasNext()){
			DBObject object = wineIdcursor.next();
			String personName = (String) object.get("personName");
			if(personNameIdMap.containsKey(personName)){
				ObjectId id = (ObjectId) object.get("_id");
				personNameIdMap.put(personName+id, id);
			}else{
				personNameIdMap.put(personName, (ObjectId) object.get("_id"));
			}
			
			int cnt = shoppingList.find(new BasicDBObject("personName",personName)).count();
			if(toBeRemoved==0){
				leastCnt = cnt;
				mostWtPerson = personName;
			}
			if(cnt < leastCnt){
				mostWtPerson = personName;
			}
			toBeRemoved++;
		}
		
		Set<Entry<String, ObjectId>> persons = personNameIdMap.entrySet();
		for(Entry<String, ObjectId> person : persons){
			if(!person.getKey().equals(mostWtPerson))
				bulk.insert(new BasicDBObject("_id",(ObjectId) person.getValue()));
		}
		
		wineIdcursor.close();
		return (toBeRemoved-1);
	}
	
	public Queue<String> getWineIds() {
		return wineIds;
	}
	
}
