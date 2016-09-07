package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 *getting list of stop words from file.txt
 */
public class StopWords implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private StopWords(){
		
		this.stopWords = new ArrayList<String>();
		BufferedReader rd = null;

//		try{
//			//get data from source file
//			rd = new BufferedReader(
//					new InputStreamReader(
//							this.getClass().getResourceAsStream("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/stopwords.txt")));
//			
//			//put each line in the list
//			String line = null;
//            while ((line = rd.readLine()) != null)
//                {
//            		this.stopWords.add(line);
//                	System.out.println(line);
//                }
//		}catch(Exception e){
//		System.out.println("Error While creating initilaizing Buffer "+e.getMessage() );
//	}
		
		try {

			String sCurrentLine;

			rd = new BufferedReader(new FileReader("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/stop-words.txt"));

			while ((sCurrentLine = rd.readLine()) != null) {
				this.stopWords.add(sCurrentLine);
				System.out.println(sCurrentLine);
			}

		} catch (IOException e) {
			
			e.printStackTrace();
		}

		// colsing reader
		finally
        {
            try {
                if (rd != null) rd.close();
            } catch (Exception ex) {}
        }
	}
	
	public static StopWords get(){
		if(_singleton == null){
			_singleton = new StopWords();
		return _singleton;
		}
		return _singleton;
	}
	
	//-----------------------------------------------------------------
	// Accessors
	//-----------------------------------------------------------------
	
    public static List<String> getWords()
    {
        return get().stopWords;
    }
	
	//------------------------------------------------------------------
	// Data Member
	//------------------------------------------------------------------
	private static StopWords _singleton;				//need one instance of stopwords
	private List<String> stopWords;						
	
}
