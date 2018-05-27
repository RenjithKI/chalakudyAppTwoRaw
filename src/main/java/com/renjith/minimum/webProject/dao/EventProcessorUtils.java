package com.renjith.minimum.webProject.dao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.renjith.minimum.webProject.domain.EventObject;




/**
 * @author Renjith Kachappilly Ittoop
 *using scanner
 */

public class EventProcessorUtils {

	public static void main(String[] args) throws IOException {

		long start = System.currentTimeMillis();

		final String textFilePath = "C:\\ECLIPSEOXYGEN\\WS\\JavaJSONproject\\src\\jsonJackson\\ex2.txt";
		
		long t1 = System.nanoTime();
		List<String>  listring =  produceValidList(textFilePath);
		printTimeused(t1, " produceValidList");
		
		long t2 = System.nanoTime();
		List<EventObject> liobj = parseJsonObjFromString(listring);	
		printTimeused(t2, " parseJsonObjFromString");
		
		long t3 = System.nanoTime();
		Map<String, Map<String, Long>> mapp = getStatsE(liobj);
		printTimeused(t3, " getStats22");
		
		printTimeused(t1, " total");
		
		
		/*long t4 = System.nanoTime();
		some(liobj);
		printTimeused(t4, " some");*/
		
		
		
		/*long timeTaken = System.currentTimeMillis()-start;		
		System.out.println("  >>>>>>>timeTaken in milli seconds>>>>>  "+timeTaken);

		long elapsedTimeInMillis = TimeUnit.SECONDS.convert((timeTaken), TimeUnit.MILLISECONDS);	  
		System.out.println("  >>>>>>>timeTaken in milli seconds>>>>>  "+elapsedTimeInMillis+"  SECONDS");*/	

		System.out.println("  >>>>>>>  "+ mapp.get("event") );
		System.out.println("  >>>>>>>  "+ mapp.get("data") );			
			
	}
	public static void printTimeused(long start, String label) {
		long timeTaken = System.nanoTime()-start;
		//long elapsedTime = TimeUnit.NANOSECONDS.convert((timeTaken), TimeUnit.NANOSECONDS);	
		long elapsed = TimeUnit.MILLISECONDS.convert((timeTaken), TimeUnit.NANOSECONDS);	
		System.out.println("  >>>>>>>>>>  "+ label + " ______________> "+ elapsed+"  Milli  SECONDS");		
	}

	public static List<String> produceValidList(final String textFilePath) throws IOException {		
		List<String> stringList = new ArrayList<>(2100);
		try (Scanner s = new Scanner(new BufferedReader(new FileReader(textFilePath))) ) {
			while (s.hasNextLine() ) {
				String st = s.nextLine().trim();
				if (isValidEventJsonLine(st)) 
				{					
					stringList.add(st);					
				}
			}
		}
		return stringList;
	}

	public static List<String> produceValidList222(final String textFilePath) throws IOException {
		Predicate<String> predicate  = (s)-> !s.isEmpty() && !s.equals("") && s.startsWith("{") && s.endsWith("}") && s.contains("event_type");
		List<String> stringList = new ArrayList<>(2100);
		try (Scanner s = new Scanner(new BufferedReader(new FileReader(textFilePath))) ) {
			while (s.hasNextLine() ) {
				String st = s.nextLine().trim();
				if (predicate.test(st)) 
				{					
					stringList.add(st);					
				}
			}
		}
		return stringList;
	}

	public static List<String> produceValidList333(final String textFilePath) throws IOException {
		Predicate<String> predicate  = (s)-> !s.isEmpty() && !s.equals("") && s.startsWith("{") && s.endsWith("}") && s.contains("event_type");
		Function<String, Predicate<String> >  stringcontainfx = pivot -> e -> e.contains(pivot);
		Function<String, Predicate<String> >  stringstartfx = pivot -> e -> e.startsWith(pivot);
		Function<String, Predicate<String> >  stringendfx = pivot -> e -> e.endsWith(pivot);
		List<String> stringList = new ArrayList<>(2100);
		try (Scanner s = new Scanner(new BufferedReader(new FileReader(textFilePath))) ) {
			while (s.hasNextLine() ) {
				String st = s.nextLine().trim();
				if (stringcontainfx.apply("event_type").test(st)    ) 
				{					
					stringList.add(st);					
				}
			}
		}
		return stringList;
	}



	public static boolean isValidEventJsonLine(String st) {		
		/*add a regular expression here in need of validation of text*/
		return !st.isEmpty() && !st.equals("") && st.startsWith("{") && st.endsWith("}") && st.contains("event_type");
	}

	public static List<EventObject> parseJsonObjFromString(List<String>  listring) {
		List<EventObject> list = new ArrayList<>(2100);		
		ObjectMapper mapper = new ObjectMapper();

		for (String line : listring) {
			try {
				EventObject objJson = mapper.readValue(line, EventObject.class);				
				list.add(objJson);
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
		return list;
	}

	public static List<EventObject> parseJsonObjFromString22(List<String>  listring) {
		List<EventObject> list = new ArrayList<>(2100);		
		ObjectMapper mapper = new ObjectMapper();

		list = listring.parallelStream().map(line -> {			
			EventObject obj = null;
			try {
				obj = mapper.readValue(line, EventObject.class);
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} 

			return obj;
		}).collect(Collectors.toList());		
		return list;
	}

	private static void some(List<EventObject> objList) {
		
		List<String> userList = objList.stream()
				.map((o) -> o.getEvent_type() )
				.collect(Collectors.toList() );
		System.out.println("_____1__________"+userList);
		
		Set<String> eventset = objList.stream()
				.map((o) -> o.getEvent_type() )
				.collect(Collectors.toSet() );

		Set<String> dataset = objList.stream()
				.map((o) -> o.getData() )
				.collect(Collectors.toSet() );
		System.out.println("_____1__________"+eventset);		 			 
		System.out.println("______2_________"+dataset);
		
		Map<String, List<EventObject>> result11 =
				objList.stream().collect(Collectors.groupingBy(EventObject::getEvent_type));
		
		System.out.println("_200_______"+result11);
		
		Map<String, List<EventObject>> result22 =
				objList.stream().collect(Collectors.groupingBy(EventObject::getData));

		Map<String, Long> result =
				objList.stream().collect(Collectors.groupingBy(EventObject::getEvent_type, Collectors.counting()));

		Map<String, Long> result20 =
				objList.stream().collect(Collectors.groupingBy(EventObject::getData, Collectors.counting()));		

		



	}


	public static Map<String, Map<String, Long>> getStats333(List<EventObject> objList) {

		Map<String, Map<String, Long> > map = new HashMap<>(2);

		ConcurrentMap<String, Long> eventStat =
				objList.parallelStream().collect(Collectors.groupingByConcurrent(EventObject::getEvent_type, (Collectors.counting()) ));		

		ConcurrentMap<String, Long> dataStat =
				objList.parallelStream().collect(Collectors.groupingByConcurrent(EventObject::getData, Collectors.counting() ));	

		map.put("event", eventStat);
		map.put("data", dataStat);
		return map;

	}

	public static Map<String, Map<String, Long>> getStats22(List<EventObject> objList) {	

		/*Map<String, Map<String, Integer> > map = new HashMap<String, Map<String, Integer> >(2);
		Map<String, Integer> eventMap = new HashMap<String, Integer>();
		Map<String, Integer> dataMap = new HashMap<String, Integer>();*/

		Map<String, Map<String, Long> > map = new HashMap<>(2);
		/*Map<String, Long> eventMap = new HashMap<>();
		Map<String, Long> dataMap = new HashMap<>();*/


		Map<String, Long> eventStat =
				objList.parallelStream().collect(Collectors.groupingBy(EventObject::getEvent_type, (Collectors.counting()) ));		

		Map<String, Long> dataStat =
				objList.parallelStream().collect(Collectors.groupingBy(EventObject::getData, Collectors.counting() ));


		map.put("event", eventStat);
		map.put("data", dataStat);
		return map;

	}

	public static Map<String, Map<String, Long>> getStats(List<EventObject> objList) {

		Map<String, Map<String, Long> > map = new HashMap<>(2);
		Map<String, Long> eventStat =
				objList.stream()
				.collect(Collectors.groupingBy(EventObject::getEvent_type,
						Collectors.counting()  )
						);		

		Map<String, Long> dataStat =
				objList.parallelStream().collect(Collectors.groupingBy(EventObject::getData, Collectors.counting() ));	

		map.put("event", eventStat);
		map.put("data", dataStat);
		return map;

	}
	
	public static Map<String, Map<String, Long>> getStatsE(List<EventObject> objList) {
/*http://www.baeldung.com/java-concurrent-map*/
		Map<String, Map<String, Long> > map = new HashMap<>(2);
		ConcurrentMap<String, Long> eventStat =
				objList.stream()
				.collect(Collectors.groupingByConcurrent(EventObject::getEvent_type,
						Collectors.counting()  )
						);		

		ConcurrentMap<String, Long> dataStat =
				objList.parallelStream().collect(Collectors.groupingByConcurrent(EventObject::getData, Collectors.counting() ));	

		map.put("event", eventStat);
		map.put("data", dataStat);
		return map;

	}


	public static Map<String, Map<String, Integer> > getStats_old(List<EventObject> objList) {	

		Map<String, Map<String, Integer> > map = new HashMap<String, Map<String, Integer> >(2);
		Map<String, Integer> eventMap = new HashMap<String, Integer>();
		Map<String, Integer> dataMap = new HashMap<String, Integer>();


		for (EventObject o : objList) {
			String eventType = o.getEvent_type();
			String data = o.getData();
			if (eventMap.containsKey(eventType)) {
				eventMap.put(eventType, eventMap.get(eventType) + 1);
			} else {
				eventMap.put(eventType, 1);
			}
			if (dataMap.containsKey(data)) {
				dataMap.put(data, dataMap.get(data) + 1);
			} else {
				dataMap.put(data, 1);
			} 
		}
		map.put("event", eventMap);
		map.put("data", dataMap);
		return map;

	}
}