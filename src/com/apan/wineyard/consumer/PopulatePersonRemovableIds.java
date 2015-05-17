package com.apan.wineyard.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.types.ObjectId;

import com.apan.wineyard.beans.PersonDuplicate;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PopulatePersonRemovableIds implements Runnable{
	DBCollection shoppingList;
	DBCollection removableIds;
	DBCollection wineweight;
	DBCollection personPriority;
	Queue<String> personNames = new ConcurrentLinkedQueue<String>();
	long deleted = 0;
	PopulateUtil populateUtil = new PopulateUtil();

	public PopulatePersonRemovableIds(DBCollection shoppingList,DBCollection removableIds,DBCollection wineweight){
		this.shoppingList = shoppingList;
		this.removableIds = removableIds;
		this.wineweight = wineweight;
		personNames = getPersonNamesList();
	}

	@Override
	public void run(){
		removePersonMoreThan3WineBottles();
	}

	private void removePersonMoreThan3WineBottles(){
		BulkWriteOperation bulk = removableIds.initializeUnorderedBulkOperation();
		int bufferCnt = 0;
		String person = null;
		try{
			while(personNames.size() > 0 && (person=personNames.remove()) != null){
				DBCursor cursor = shoppingList.find(new BasicDBObject("personName",person));
				TreeSet<PersonDuplicate> personWineWt = new TreeSet<PersonDuplicate>();
				while(cursor.hasNext()){
					DBObject dbObj = cursor.next();
					String wineId = (String)dbObj.get("wineId");
					DBObject wineWtObj = (DBObject)wineweight.findOne(new BasicDBObject("wineId",wineId));
					personWineWt.add(new PersonDuplicate((Integer)wineWtObj.get("wineWeight"), (ObjectId)dbObj.get("_id")));
				}
				cursor.close();
				List<PersonDuplicate> list = new ArrayList<PersonDuplicate>(personWineWt);
				for(int i = (list.size() - 1);i>2;i--){
					bulk.insert(new BasicDBObject("_id",list.get(i).getId()));
					bufferCnt++;

					if(bufferCnt > 0 && (bufferCnt % 10000 == 0)){
						bulk.execute();
						bufferCnt = 0;
						System.out.println("Inserted into removableIds :: "+ removableIds.getCount()+" records");
						bulk = removableIds.initializeUnorderedBulkOperation();
					}
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		if(bufferCnt > 0){
			bulk.execute();
		}
		System.out.println("Inserted into removableIds :: "+ removableIds.getCount()+" records");
	}

	Queue<String> getPersonNamesList(){
		Cursor personNameCursor = populateUtil.groupByField("$personName",shoppingList,100000);
		while(personNameCursor.hasNext()){
			DBObject object = personNameCursor.next();
			Integer count = (Integer) object.get("count");
			if(count > 3){
				personNames.add((String) object.get("_id"));
			}
		}
		personNameCursor.close();

		System.out.println(personNames.size()+" personNames have more than 3 wine bottles");

		return personNames;
	}


	public Queue<String> getPersonNames() {
		return personNames;
	}
	
	
	
}
