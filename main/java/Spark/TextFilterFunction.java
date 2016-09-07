package spark;

import org.apache.spark.api.java.function.Function;


/*
 * delete non-alphabetic caracteres
 * put all caractere to lowercase
 */
public class TextFilterFunction implements Function<DataFields, DataFields>{
	
	private static final long serialVersionUID = 1L;
	//@Override
	public DataFields call(DataFields datafields){
		System.out.println("****************filter function****************");
		String text = datafields.getText();
		text = text.replaceAll("[^a-zA-Z0-9|#\\s]", "").trim().toLowerCase();
		datafields.setText(text);
		System.out.println(text);
        return datafields;
	}

}
