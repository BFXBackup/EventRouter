package com.buysidefx.eventrouter.processors;

import com.buysidefx.eventrouter.events.Event;

public interface EventProcessorInterface {

	public Boolean processEvent(Event e);
	
}
