package core.base;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 *Containes static methods utils for diffirent classes
 */
public class Utils {
	/*
	 * transform a json file to string
	 */
	public static String getStringFromJsonFile(File file){
		
		JSONParser parser = new JSONParser();
		String json="";
		
		try {
            System.out.println("Reading JSON file from Java program");
            FileReader fileReader = new FileReader(file);
            JSONObject js = (JSONObject) parser.parse(fileReader);


             json=js.toString();
             System.out.println("jsonText : "+json);
             
             return json;
             
        } catch (IOException | ParseException ex) {
            ex.printStackTrace();
		}
		
		return null;
	}
	
	/*
	 * transform a given string date to (Jun-23-2016)
	 */
	
	public static String ToFormatDate(String dateToChange){
		
		String[] listDateComponent;
		String date;
		String monthLettre="";
		String monthNumber="";
		
		if(!dateToChange.equals("null")){
			listDateComponent=dateToChange.split(" ");
			monthLettre = listDateComponent[1];
			if(monthLettre.equals("Jan")){
				monthNumber="01";
			}else if(monthLettre.equals("Feb")){
				monthNumber="02";
			}else if(monthLettre.equals("Mar")){
				monthNumber="03";
			}else if(monthLettre.equals("Apr")){
				monthNumber="04";
			}else if(monthLettre.equals("May")){
				monthNumber="05";
			}else if(monthLettre.equals("Jun")){
				monthNumber="06";
			}else if(monthLettre.equals("Jul")){
				monthNumber="07";
			}else if(monthLettre.equals("Aug")){
				monthNumber="08";
			}else if(monthLettre.equals("Sep")){
				monthNumber="09";
			}else if(monthLettre.equals("Oct")){
				monthNumber="10";
			}else if(monthLettre.equals("Nov")){
				monthNumber="11";
			}else if(monthLettre.equals("Dec")){
				monthNumber="12";
			}
			
			date=listDateComponent[5]+"-"+monthNumber+"-"+listDateComponent[2];
		}else{
			date="null";
		}
		return date;
			
	}

}
