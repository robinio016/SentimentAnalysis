//risque des exceptions liées à des valeurs non définis dans le fichier json: il faut implementer des foncions pour palier ce problème
package core.dataTransform;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;

/**
 * Transform a raw tweet to clean version suitable for reporting activities
 */

public class TwitterTransform extends AbstractTransform {
	
	/**
	 * Constructors
	 */
	
	//------------------------------------------------------------------------------
	//			Constructor
	//-------------------------------------------------------------------------------
	
	public TwitterTransform(){
		setParameters();
	}
	
	//------------------------------------------------------------------------------
	//			Implementation
	//-------------------------------------------------------------------------------
	private void setParameters(){
		
	}
	
	/**
	 * transform a tweet to clean object ready for reporting
	 */
	@Override
	public Object transform(String data) {
		
		JSONObject transformedData = new JSONObject();
		
		try{
			JSONObject jsonData = new JSONObject(data);

		      // Setting id,id_origin,origin,type
		      transformedData.put("id", jsonData.get("id"));  
		      transformedData.put("id_origin", jsonData.get("id_origin"));
		      transformedData.put("origin", "twitter");
		      transformedData.put("type", jsonData.get("type"));

		      // Setting web source
		      transformedData.put("web_source",jsonData.isNull("source") ? JSONObject.NULL : jsonData.getString("source"));

		      // Setting Text,sentimentText
		      String text = jsonData.get("text").toString();
		      transformedData.put("text", text);
		      transformedData.put("sentimentText",text); // this field contain a specific cleaning to sentiment analysis

		      // Setting lang
		      transformedData.put("lang", jsonData.get("lang"));

		      // Getting keywords
		      //transformedData.put("keywords", Utils.getkeywordsFromText(keywords, text));

		      // Getting category
		      //transformedData.put("category", Utils.getTextGroup(text,"default",categories));

		      // Getting hashtags
		      transformedData.put("hashtags",JSONObject.NULL);
		      if(!jsonData.isNull("entities")){
		        JSONObject entities = jsonData.getJSONObject("entities");
		        if(!entities.isNull("hashtags") && entities.getJSONArray("hashtags").length() != 0){
		          transformedData.put("hashtags", getHashtagsFromEntities(entities.getJSONArray("hashtags")));
		        }
		      } 

		      // Setting likes, dislikes, shares
		      transformedData.put("likes", jsonData.isNull("favorite_count") ? JSONObject.NULL : Integer.parseInt(jsonData.getString("favorite_count")));
		      transformedData.put("dislikes", JSONObject.NULL);
		      transformedData.put("shares", jsonData.isNull("retweet_count") ? JSONObject.NULL : Integer.parseInt(jsonData.getString("retweet_count")));

		      // Setting time
		      transformedData.put("time", jsonData.isNull("created_at") ? JSONObject.NULL : jsonData.get("created_at"));
		          

		      // Getting location 
		      transformedData.put("location", jsonData.isNull("place") ? JSONObject.NULL : getLocationFromPlace(jsonData.getJSONObject("place")));

		      // Setting user
		      transformedData.put("user", jsonData.isNull("user") ? JSONObject.NULL : getUserInformationFromJson(jsonData.getJSONObject("user")));
			
			
		}catch(Exception e){
			System.out.println("Error while Trnsforming Data : "+e.getMessage());
		}
		
		return transformedData.toString();
	}
	
	  /**
	   * Retrieve hashtags from entities
	   * @param hashtags
	   * @return
	   */
	  private Object getHashtagsFromEntities(JSONArray hashtags) {
		    JSONArray result = new JSONArray();

		    for(int i = 0; i<hashtags.length(); i++) {
		      JSONObject hashtagsObject = hashtags.getJSONObject(i);
		      JSONObject hashtagsObjectResult = new JSONObject();

		      if (!hashtagsObject.isNull("text")) {
		        hashtagsObjectResult.put("value", hashtagsObject.getString("text"));
		        result.put(hashtagsObjectResult);
		      }
		    }

		    return result.length() < 0 ? JSONObject.NULL : result;
		  }
	  
	  /**
	   * return user information if user filed exist or return null
	   * @param raw
	   * @return
	   * @throws JSONException
	   */
	  private JSONObject getUserInformationFromJson(JSONObject rawUser) {
	    JSONObject user = new JSONObject();
	    user.put("screen_name",rawUser.get("screen_name"));
	    return user;
	  }
	  
	  
	  /**
	   * Retrieves location informations from incoming place.
	   * @param place
	   * @return
	   */

	  private JSONObject getLocationFromPlace(JSONObject place){
	    JSONObject result = new JSONObject();
	    JSONArray placeLocation = new JSONArray();
	    String countryCode;
	    String countryName;
	    double longitude;
	    double latitude;

	    countryCode = place.get("country_code").toString();
	    countryName = place.get("name").toString();
	    longitude = place.getJSONObject("bounding_box")
	        .getJSONArray("coordinates")
	        .getJSONArray(0)
	        .getJSONArray(0)
	        .getDouble(0);

	    latitude = place.getJSONObject("bounding_box")
	        .getJSONArray("coordinates")
	        .getJSONArray(0)
	        .getJSONArray(0)
	        .getDouble(1);

	    placeLocation.put(longitude);
	    placeLocation.put(latitude);

	    result.put("country_code", countryCode);
	    result.put("full_name", place.get("full_name"));
	    result.put("place_type", place.get("place_type"));
	    result.put("url", place.get("url"));
	    
	    //il faut implementer des fonctions pour récuperer les noms des villes à partir des coordonées
	    
	    //result.put("country", getCountryName(countryCode));
	    //result.put("name", getCityName(countryName, countryCode, longitude, latitude));
	    result.put("place_location", placeLocation);
	    
	    		

	    return result;
	  }
	  

	
	//------------------------------------------------------------------------------
	//			Data member
	//-------------------------------------------------------------------------------
	
	String Keywords;
	

}
