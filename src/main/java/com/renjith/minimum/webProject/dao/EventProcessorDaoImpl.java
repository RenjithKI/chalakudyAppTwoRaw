package com.renjith.minimum.webProject.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import com.renjith.minimum.webProject.domain.EventObject;
/**
 * @author Renjith Kachappilly Ittoop
 *
 */
@Component
@Primary
public class EventProcessorDaoImpl implements EventProcessorDao {	

	private Map<String, Map<String, Long>> map = new HashMap<>(2);
	final String textFilePath;

	@Autowired
	public EventProcessorDaoImpl(@Value( "${textFile.path}" ) final String textFilePath) throws IOException{
		this.textFilePath = textFilePath;		

		List<String>  listring =  EventProcessorUtils.produceValidList(textFilePath);
		List<EventObject> liobj = EventProcessorUtils.parseJsonObjFromString(listring);		
		map = EventProcessorUtils.getStats(liobj);	
		
		
	}

	public static void printTimeused(long start, String label) {
		long timeTaken = System.nanoTime()-start;
		//long elapsedTime = TimeUnit.NANOSECONDS.convert((timeTaken), TimeUnit.NANOSECONDS);	
		long elapsed = TimeUnit.MILLISECONDS.convert((timeTaken), TimeUnit.NANOSECONDS);	
		System.out.println("  >>>>>>>>>>  "+ label + " ______________> "+ elapsed+"  Milli  SECONDS");		
	}

	



	@Override
	public Map<String, Long> getEventMap() {		
		return map.get("event");
	}

	@Override
	public Map<String, Long> getDataMap() {		
		return map.get("data");
	}
}