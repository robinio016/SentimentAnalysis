package core.dataSource;

import core.dataContainer.AbstractContainer;

/*
 * represents an abstraction of all dataSource implementations
 * holds a commons mechanisms related to data source such connection, read and write operation
 */
public abstract class AbstractSource {
		
	//-------------------------------------------------------------
	//   constructor
	//------------------------------------------------------------
	
	/**
	 * given a data container
	 * @param data container that will store data from data Source
	 */
	public AbstractSource(AbstractContainer container){
		this.container = container;
	}
	
	//--------------------------------------------------------------------
	//   Abstraction
	//---------------------------------------------------------------------
	
	/*
	 * connect
	 */
	public abstract void connect();
	
	//------------------------------------------------------------------------
	//   Accessors
	//------------------------------------------------------------------------
	
	protected AbstractContainer getContainer(){
		return this.container;
	}
	
	protected void setContainer(AbstractContainer conatainer){
		this.container=container;
	}
	
	protected String getKeyWords(){
		return this.keyWords;
	}
	
	protected void setKeyWord(String keyWords){
		this.keyWords= keyWords;
	}
	
	//---------------------------------------------------------------------
	//   data member
	//-----------------------------------------------------------------------
	
	private AbstractContainer container; //could be one of the various implementation found in dataContainer package
	protected String keyWords;

}
