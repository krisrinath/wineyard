package com.apan.wineyard.beans;

import org.bson.types.ObjectId;

public class PersonDuplicate implements Comparable<PersonDuplicate>{

	ObjectId id;
	int weight;
	
	public PersonDuplicate(int weight,ObjectId id){
		this.id = id;
		this.weight = weight;
	}
	
	public ObjectId getId() {
		return id;
	}
	public void setId(ObjectId id) {
		this.id = id;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	
	@Override
	public int compareTo(PersonDuplicate o) {
		return this.weight > o.weight ? 1 : -1;
	}
	
	public String toString(){
		return "id :: "+id+" :: wt "+weight;
	}
}
