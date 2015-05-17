package com.apan.wineyard.consumer;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class PopulateUtil {

	Cursor groupByField(String field,DBCollection collection,Integer batchsize){
		List<DBObject> conditions = new ArrayList<DBObject>();
		BasicDBObject groupBy = new BasicDBObject("_id",field).append("count",new BasicDBObject("$sum",1));
		BasicDBObject obj = new BasicDBObject("$group",groupBy);
		conditions.add(obj);

		BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("count",-1));
		conditions.add(sort);

		AggregationOptions aggregationOptions = AggregationOptions.builder()
				.batchSize(batchsize)
				.outputMode(AggregationOptions.OutputMode.CURSOR)
				.allowDiskUse(true)
				.build();

		return collection.aggregate(conditions,aggregationOptions);
	}
}
