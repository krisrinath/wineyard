package com.apan.wineyard.delete;

import java.util.LinkedList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class DeleteDuplicates{
	DBCollection removableIds;
	DBCollection shoppingList;
	private long deleted = 0;
	
	public DeleteDuplicates(DBCollection removableIds,DBCollection shoppingList){
		this.removableIds = removableIds;
		this.shoppingList = shoppingList;
	}

	public void deleteDuplicates() {
		DBCursor removeCursor = removableIds.find();
		removeCursor = removeCursor.batchSize(10000);
		List<ObjectId> Ids = new LinkedList<ObjectId>();
		while(removeCursor.hasNext()){
			if(!Ids.isEmpty() && Ids.size()%10000 == 0){
				bulkRemove(Ids);
			}
			Ids.add((ObjectId)removeCursor.next().get("_id"));
		}
		
		if(!Ids.isEmpty() && Ids.size() > 0)
			bulkRemove(Ids);
		removeCursor.close();
	}
	
	void bulkRemove(List<ObjectId> ids){
		BulkWriteOperation bulk = this.shoppingList.initializeUnorderedBulkOperation();
		BasicDBObject dbObject = new BasicDBObject("_id",new BasicDBObject("$in",ids));
		bulk.find(dbObject).remove();
		BulkWriteResult result = bulk.execute();
		deleted = deleted + result.getRemovedCount();
		System.out.println("Deleted so far :: "+deleted+ " Remaining cnt to be deleted:: "+(removableIds.getCount() - deleted));
		ids.clear();
	}
}
