package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class NegativeWords {
	
	//-----------------------------------------------------------------
	//			Constructor
	//-----------------------------------------------------------------
	private NegativeWords(){
		this.negWords = new HashSet<String>();
        BufferedReader rd = null;
//        try
//        {
//            rd = new BufferedReader(
//                new InputStreamReader(
//                    this.getClass().getResourceAsStream("src/main/resources/negwords.txt")));
//            String line;
//            while ((line = rd.readLine()) != null)
//            {
//            	this.negWords.add(line);
//            }
//                
//        }
//        catch (IOException ex)
//        {
//            System.out.println("IO error while initializing " + ex.getMessage());
//        }
		try {

			String sCurrentLine;

			rd = new BufferedReader(new FileReader("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/neg-words.txt"));

			while ((sCurrentLine = rd.readLine()) != null) {
				this.negWords.add(sCurrentLine);
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
	
	
	//-----------------------------------------------------------
	//			Accessors
	//------------------------------------------------------------
	
	public static NegativeWords get(){
		if(_singleton==null){
			_singleton=new NegativeWords();
			return _singleton;
		}
		return _singleton;
	}
	
	public static Set<String> getWords(){
		return get().negWords;
	}
	
	//------------------------------------------------------------
	//			DataMember
	//------------------------------------------------------------
	private Set<String> negWords;
    private static NegativeWords _singleton;
}
