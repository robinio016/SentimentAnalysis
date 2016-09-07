package es;

import core.base.Properties;
import core.dataContainer.ElasticSearchContainer;

public class ESTestEventContainer {
	
	public static void main(String[]args){
		/*
		 * getting keywords
		 * create type with that keyword
		 * read json file from folder with name of this keyword 
		 */
		String keywords =Properties.getString("twitter.keyword","es");
		String pathToJson =Properties.getString("elasticSearch.toElasticSearchFrom", "es")
							+"/"
							+keywords
							+"/";
		ElasticSearchContainer esContainer = new ElasticSearchContainer();
		esContainer.WriterConnect();
		esContainer.setIndex("usaa");
		esContainer.setType(keywords);
		esContainer.createIndex();
		esContainer.saveFilesToElasticSearch(pathToJson);
				
		}

}
