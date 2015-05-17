package com.apan.wineyard;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.apan.wineyard.consumer.PopulatePersonRemovableIds;
import com.apan.wineyard.consumer.PopulateWineRemovableIds;
import com.apan.wineyard.delete.DeleteDuplicates;
import com.apan.wineyard.output.PrintOutput;
import com.apan.wineyard.producer.InsertIntoDB;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class WineAllocationThreads {

	public static void main(String args[]){
		WineAllocationThreads threads = new WineAllocationThreads();
		MongoClient client = null;
		try {
			//Can use sl4j logging but using console output for convinience
			
			System.out.println("Start time :: "+new Date());
			System.out.println("Connection to Mongo \"wineyard\" DB");
			client = new MongoClient();
			DB db = client.getDB("wineyard");
			
			System.out.println("Creating a collection \"shoppinglist\" to store all the wine bottle requests");
			DBCollection shoppingList = db.getCollection("shoppinglist");

			String inputFile = args[0];
			InsertIntoDB dbInsert = new InsertIntoDB(inputFile, shoppingList);
			
			System.out.println("Starting to Insert records inserted into the DB");
			Thread producer = new Thread(dbInsert);
			producer.start();
			producer.join();
			System.out.println("Insert completed ::  "+shoppingList.getCount()+" records inserted into the DB");
			System.out.println("Insert completed time :: "+new Date());
			
			System.out.println("Creating an shoppingList index on wineId & personName columns");
			shoppingList.createIndex(new BasicDBObject("wineId", 1),"wineIdIndex");
			shoppingList.createIndex(new BasicDBObject("personName", 1),"personNameIndex");
			System.out.println("Index creation completed");
			
			System.out.println("Creating a collection \"removableIds\" to store all the records that needs to be removed");
			DBCollection removableIds = db.getCollection("removableIds");
			System.out.println("Creating a collection \"wineweight\" to store wineweight.Least sold wine bottle has the maximum weight");
			
			DBCollection wineweight = db.getCollection("wineweight");
			
			System.out.println("Populating duplicate wineIds to make it distinct");
			PopulateWineRemovableIds wineRemovableIds = new PopulateWineRemovableIds(shoppingList,removableIds,wineweight);
			//Starting 5 threads to find the duplicate ids that needs to be removed
			threads.submitTasks(wineRemovableIds,5);
			System.out.println("Populating duplicate wineIds done");
			System.out.println("Removable Ids count ::"+ removableIds.count());
			wineRemovableIds.getWineIds().clear();
			
			
			System.out.println("Deleting the wineDuplicateIds from shoppingList collection");
			System.out.println("Dropping wineIdIndex for delete operation to make it faster");
			shoppingList.dropIndex("wineIdIndex");
			System.out.println("Dropped wineIdIndex");
			DeleteDuplicates delete = new DeleteDuplicates(removableIds,shoppingList);
			delete.deleteDuplicates();
			System.out.println("Duplicate wineIds Deletion completed :: "+new Date());
			removableIds.drop();
			removableIds = db.getCollection("removableIds");
			System.out.println("Removable Ids count ::"+ removableIds.count());
			
			System.out.println("Populating removable ids with person > 3 wine bottles");
			PopulatePersonRemovableIds personRemovableIds = new PopulatePersonRemovableIds(shoppingList,removableIds,wineweight);
			//Starting 5 threads to find the duplicate ids that needs to be removed
			threads.submitTasks(personRemovableIds,5);
			System.out.println("Removable ids with person > 3 wine bottles done");
			System.out.println("Removable Ids count ::"+ removableIds.count());
			personRemovableIds.getPersonNames().clear();
			
			
			System.out.println("Deleting the personIds > 3 wine bottles from shoppingList collection");
			System.out.println("Dropping personNameIndex for delete operation to make faster");
			shoppingList.dropIndex("personNameIndex");
			System.out.println("Dropped personNameIndex");
			delete = new DeleteDuplicates(removableIds,shoppingList);
			delete.deleteDuplicates();
			System.out.println("person > 3 bottles Deletion completed :: "+new Date());
			System.out.println("Removable Ids count ::"+ removableIds.count());
			
			System.out.println("Total wine bottles sold "+shoppingList.getCount());

			System.out.println("Printing started :: "+new Date());
			String outputFile = args[1];
			PrintOutput print = new PrintOutput(shoppingList, outputFile);
			print.printOutput();
			System.out.println("Printing finished :: "+new Date());
			System.out.println("End time :: "+new Date());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch(Exception ex){
			ex.printStackTrace();
		}
	}

	<T extends Runnable> void submitTasks(T task,int threadPoolSize){
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		for(int i= 0;i < threadPoolSize ;i++){
			executor.submit(task);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			System.out.println("Interuppted");
		}

	}
}
