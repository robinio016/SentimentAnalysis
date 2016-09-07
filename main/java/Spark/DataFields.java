package spark;

public class DataFields {
	//--------------------------------------------------------------------
	//			Constructors
	//---------------------------------------------------------------------
	public DataFields(){
			id = 0; 
		 	text="null";
		 	origin="null";
		 	createdAt="null";
			location="null";
		 	retweetedCount=0;
			favoriteCount=0;
		 	positiveScore=0;
			negativeScore=0;
		 	score="";
	}
	
	//---------------------------------------------------------------------
	//			Data Accessors
	//----------------------------------------------------------------------
	public long getId(){
		return this.id;
	}
	
	public void setId(long id){
		this.id=id;
	}
	
	public String getText(){
		return this.text;
	}
	
	public void setText(String text){
		this.text=text;
	}
	
	public String getOrigin(){
		return this.origin;
	}
	
	public void setOrigin(String origin){
		this.origin=origin;
	}
	
	public String getCreatedAt(){
		return this.createdAt;
	}
	
	public void setCreatedAt(String createdAt){
		this.createdAt=createdAt;
	}
	
	public String getLocation(){
		return this.location;
	}
	
	public void setLocation(String location){
		this.location=location;
	}
	
	public int getRetweetedCount(){
		return this.retweetedCount;
	}
	
	public void setRetweetedCount(int retweetedCount){
		this.retweetedCount=retweetedCount;
	}
	
	public int getFavoriteCount(){
		return this.favoriteCount;
	}
	
	public void setFavoriteCount(int favoriteCount){
		this.favoriteCount=favoriteCount;
	}
	
	public float getPositiveScore(){
		return this.positiveScore;
	}
	
	public void setPositiveScore(float positiveScore){
		this.positiveScore=positiveScore;
	}
	
	public float getNegativeScore(){
		return this.negativeScore;
	}
	
	public void setNegativeScore(float negativeScore){
		this.negativeScore=negativeScore;
	}
	
	public String getScore(){
		return this.score;
	}
	
	public void setScore(String score){
		this.score=score;
	}
	
	//---------------------------------------------------------------------
	//			Data Member
	//----------------------------------------------------------------------
	private long	id ; 
	private String 	text;
	private String 	origin;
	private String 	createdAt;
	private String 	location;
	private int 	retweetedCount;
	private int 	favoriteCount;
	private float 	positiveScore;
	private float 	negativeScore;
	private String 	score;
}
