package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import scala.Tuple10;

public class FileWriter implements Function2<JavaRDD<Tuple10<Long,String,String,String,String,String,String,Float,Float,String>>
												,Time,Void> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Void call(JavaRDD<Tuple10<Long, String,String,String,String,String,String, Float, Float, String>> rdd, Time time) throws Exception {
		System.out.println("---------- " + rdd.count());
		//save data to file
		if (rdd.count()<=0){
			return null;
		}
		String path ="src/main/resources/output_spark";
		rdd.saveAsTextFile(path);
		return null;
	}
	
}
