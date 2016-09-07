package core.dataTransform;


/**
 * Abstraction of transformation process
 */
public abstract class AbstractTransform {

	/**
	 * Transformation logic
	 * @param data row 			:	data got from a container need to be cleaned up in order to be used for reporting
	 * @return transformed data :	data ready for reporting 
	 */
	
	public abstract Object transform(String data);
}
