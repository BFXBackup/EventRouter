package com.buysidefx.eventrouter.registry;


import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.buysidefx.eventrouter.processors.EventProcessorInterface;
import com.buysidefx.eventrouter.processors.NewOrderEventProcessor;

public class EventProcessorRegistry {

	// SCOTT - this PERIOD should come in from a properties file - 
	// look in the RuleServer for how to do properties

	public static final long PERIOD = 5000;
	Map<String, ArrayList<EventProcessorInterface>> registry = new HashMap<>();

	public EventProcessorRegistry() {

		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
				new EventPollingRunnable(), 0, PERIOD, TimeUnit.MILLISECONDS);


	}



	// SCOTT - this is how you add EventProcessors to the lists (or create entirely new short code lists)
	public void registerEventProcessor(EventProcessorInterface ep, List<String> eventShortCodes) throws Exception {

		for (String s : eventShortCodes) {
			// create a short code list if doesn't exist

			// SCOTT - when dealing with a data structure from multiple threads it has to be synchronized while
			// being accessed.  "registry" is accessed by multiple threads.
			synchronized(registry) {
				if (!registry.containsKey(s)) {
					// SCOTT - if the shot code doesn't exist as a key in the hashmap then add it with a new, empty list.
					registry.put(s, new ArrayList<EventProcessorInterface>());
				}

				// add the event processor to the list
				registry.get(s).add(ep);
				System.out.println("REGISTRY: "+registry);
			}
		}
	}




	// SCOTT - this is how you remove event processors from the list
	public void unregisterEventProcessor(EventProcessorInterface ep, List<String> eventShortCodes) throws Exception {
		for (String s : eventShortCodes) {
			synchronized(registry) {
				// remove the event processor if it is in the list
				if (registry.containsKey(s)) {
					int index = registry.get(s).indexOf(ep);
					if (index != -1) {
						registry.remove(index);
						System.out.println("REGISTRY: "+registry);
					}
				}
			}
		}
	}





	private class EventPollingRunnable implements Runnable {

		public void run() {
			String format = "Ran at %1$tF %<tT,%<tL;";
			System.out.println(String.format(format, System.currentTimeMillis()));
			// SCOTT - Use the DatabaseUtilitySingleton from RulesServer to create the
			// database utilites you need to get the new events and put then into the
			// appropriate list in the registry (remember to synchronize)

			// For each event, walk the list of EventProcessors and call "processEvent(event, shortCode)"


		}


	}


	public static void main(String[] args) {

		final List<String> shortCodes = new ArrayList<>();
		shortCodes.add("NO");



		final EventProcessorRegistry epr = new EventProcessorRegistry();

		final NewOrderEventProcessor nep = new NewOrderEventProcessor();

		Thread registrationThread = new Thread() {
			public void run() {
				try {
				epr.registerEventProcessor(nep,shortCodes);
				try {
					Thread.sleep(5000);
				} catch(InterruptedException ie) {// ignore
				}
				
				epr.unregisterEventProcessor(nep, shortCodes);
				try {
					Thread.sleep(5000);
				} catch(InterruptedException ie) {// ignore
				}
				} catch (Exception e) {
					System.out.println("EXCEPTION: "+e);
					e.printStackTrace();
				}

			}
		};
		registrationThread.start();
		
		

	}

}


