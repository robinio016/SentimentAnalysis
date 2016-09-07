package core.dataContainer;

import java.io.IOException;

public class HdfsFactory extends AbstractFactory{

	@Override
	public AbstractContainer createContainer() throws IOException {
		
			return new HdfsContainer();

	}

}
