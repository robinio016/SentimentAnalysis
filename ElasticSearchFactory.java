package core.dataContainer;

import java.io.IOException;

public class ElasticSearchFactory extends AbstractFactory{

	@Override
	public AbstractContainer createContainer() throws IOException {
		// TODO Auto-generated method stub
		return new ElasticSearchContainer();
	}
}

