package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 
 * transform words from a file.txt to a list
 *
 */
public class PositiveWords {
	
	//---------------------------------------------------------------
	//			Constructor
	//---------------------------------------------------------------
	
	private PositiveWords(){
		
		this.posWords = new HashSet<String>();
		BufferedReader rd =null;
		
		/**
		 * put positive words got from pos-words.txt in ressource file unto posWords set
		 */
//		try{
//			rd =new BufferedReader(
//					new InputStreamReader(this.getClass().getResourceAsStream("src/main/resources/pos-words.txt")));
//			String line;
//			while((line= rd.readLine()) != null){
//				this.posWords.add(line);
//			}
//		}catch(Exception e){
//			System.out.println("Error Initilizing reader for Posititive word : "+e.getMessage());
//		}
		try {

			String sCurrentLine;

			rd = new BufferedReader(new FileReader("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/pos-words.txt"));

			while ((sCurrentLine = rd.readLine()) != null) {
				this.posWords.add(sCurrentLine);
				System.out.println(sCurrentLine);
			}

		} catch (IOException e) {
			
			e.printStackTrace();
		}
		finally
        {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
	}
	
	//---------------------------------------------------------------
	//		Accessors
	//----------------------------------------------------------------
	
	/*
	 * get unique instance of positiveWords
	 */
	public static PositiveWords get(){
		if (_singleton == null){
			_singleton = new PositiveWords();
			return _singleton;
		}
		return _singleton;
	}
	
	
	//getting list of positive words
	public static Set<String> getWords(){
		return get().posWords;
	}
	
	
	
	
	//-------------------------------------------------------------------
	//			DataMember
	//-------------------------------------------------------------------
	private static PositiveWords _singleton ;					//one instance
	private  Set<String> posWords;						//list of positive words
}
