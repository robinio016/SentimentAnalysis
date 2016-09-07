package core.dataContainer;

import java.io.IOException;

import core.base.Properties;

public abstract class AbstractFactory {
	
	public static AbstractFactory getFactory(){
		
		String container = Properties.getString("sparkOuput.container", "spark");
			if(container.equals("hdfs")){
				return new HdfsFactory();
			}
			else{
				return new ElasticSearchFactory();
			}
	}
	public abstract AbstractContainer createContainer() throws IOException; 

}
