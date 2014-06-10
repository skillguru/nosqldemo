package demo;

import java.net.UnknownHostException;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

/**
 * 
 * 
 * @author Vinay
 *
 */
public class HelloMongoDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		HelloMongoDB helloMongo = new HelloMongoDB();
		helloMongo.insertData();
		helloMongo.findData();
		helloMongo.updateData();
	}

	/**
	 * An example of find data in mongo db
	 */
	private void findData(){

		BasicDBObject basic = new BasicDBObject("name" , "USA")
		.append("type", "country");
		

		
		try {
			Mongo mongo = new Mongo("127.0.0.1", 27017);
			DB db = mongo.getDB("hellomongo");
			DBCursor cursor = db.getCollection("hellomongo").
				find(basic).limit(5);

			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	
	/**
	 * An example of update data in mongo db
	 */
	private void updateData(){

		BasicDBObject basic = new BasicDBObject("name" , "USA")
		.append("type", "country")
		
		.append("count", 3)
		.append("states", 
					new BasicDBObject("1" , "MA 3")
						.append("2", "TX 3")
						.append("3", "CA 3"));

		
		try {
			Mongo mongo = new Mongo("127.0.0.1", 27017);
			DB db = mongo.getDB("hellomongo");

			
			BasicDBObject newObject = new BasicDBObject("name" , "USA")
			.append("type", "country")
			.append("count", 3)
			.append("states", 
						new BasicDBObject("1" , "MA 1999")
							.append("2", "TX 1999")
							.append("3", "CA 1999"));
			
			db.getCollection("hellomongo").update(basic, newObject);
			

			BasicDBObject newObject2 = new BasicDBObject("name" , "USA")
			.append("type", "country")
			.append("count", 3)	;

			
			DBCursor cursor2 = db.getCollection("hellomongo").
					find(newObject2);
				
				while (cursor2.hasNext()) {
					
					System.out.println(cursor2.next());
					
					
				}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * An example of inserting data in mongo db
	 */
	private void insertData(){
		
		try {
			
			//Connect to Mongo DB
			Mongo mongo = new Mongo("127.0.0.1", 27017);
			
			// by default 3 databases are created local, admin, todo
			System.out.println(" Connected "+mongo.getDatabaseNames().toString());
			
			//Created a new database 
			DB db = mongo.getDB("hellomongo");
			
			Set<String> collections = db.getCollectionNames();
			for(String s : collections){
				System.out.println("Collection: " +s);
			}
			DBCollection items = db.getCollection("hellomongo");

			long timeInsertStart = System.currentTimeMillis();
			for(int i= 0; i<100 ; i++){
			
				items.insert(createObjectsToInsert(i));
			}
			long timeInsertEnd = System.currentTimeMillis();
			System.out.println(" Time to Insert --> "+(timeInsertEnd-timeInsertStart));
			
/*			DBObject myDoc = items.findOne();
			System.out.println(myDoc);
*/

			long timeFetchStart = System.currentTimeMillis();
			DBCursor cursor = items.find();
			long timeFetchEnd = System.currentTimeMillis();
			System.out.println(" Time to fetch --> "+(timeFetchEnd-timeFetchStart));
			while (cursor.hasNext()) {
				//System.out.println(cursor.next());
			}
			
		} catch (Exception e) {
			
			e.printStackTrace();
			
		}

	}
	
	private static BasicDBObject createObjectsToInsert(int count){
		
		BasicDBObject basic = new BasicDBObject("name" , "USA")
									.append("type", "country")
									.append("count", count)
									.append("states", 
												new BasicDBObject("1" , "MA "+count)
													.append("2", "TX "+count)
													.append("3", "CA "+count));
		
		return basic;
	}
	
}
