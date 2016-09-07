package core.base;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 *	get a specific file of properties 
 */

public class Properties {
	//-------------------------------------------------------------
	//			Constructors
	//-------------------------------------------------------------
	/*
	 * load propertiesa file
	 */
	private Properties(){
		
		try{
			this.config = new PropertiesConfiguration("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/config/config.properties");
			
		}catch(Exception e){
			System.out.println("Error while loading config.properties : "+e.getMessage());
		}
	}
	
	/*
	 * load specific file properties
	 * @param String path to properties file
	 */
	private Properties(String file){
		try{
			if (file=="twitter"){
				this.config = new PropertiesConfiguration("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/config/twitter.properties");
			}
			else if(file=="es"){
				this.config = new PropertiesConfiguration("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/config/es.properties");

			}
			else if(file=="kafka"){
				this.config = new PropertiesConfiguration("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/config/kafka.properties");

			}
			else if(file=="spark"){
				this.config = new PropertiesConfiguration("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/config/spark.properties");

			}
		}catch(Exception e){
			System.out.println("Error while loading config.properties : "+e.getMessage());
		}
	}
	
	//--------------------------------------------------------------
	//			Accessors
	//--------------------------------------------------------------
	public static Properties get(){
		if(singleton == null)
			singleton = new Properties();
		
		return singleton;
			
	}
	
	/*
	 * get one instance of a specific file properties
	 */
	public static Properties get(String propertiesFile){
		if(singleton == null)
			singleton = new Properties(propertiesFile);
		
		return singleton;
			
	}
	
	public static String getString(String key){
			
			return get().config.getString(key);
		}
	/*
	 * surchage pour avoir une instance specifique
	 */
	public static String getString(String key,String propertiesfile){
		
		return get(propertiesfile).config.getString(key);
	}
	
	public static Integer getInt(String key){
		
		return get().config.getInteger(key,null);
	}
	
	public static Integer getInt(String key,String propertiesfile){
		
		return get(propertiesfile).config.getInt(key);
	}
	//--------------------------------------------------------------
	//			Data member
	//--------------------------------------------------------------
	 private static Properties singleton ;
	 private Configuration config;

}
