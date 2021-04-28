package com.infogain.gcp.poc.component;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.infogain.gcp.poc.entity.PNREntity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MessageConverter {

	@Autowired
	private Gson gson;

	public String convert(PNREntity pnrEntity) {
		String result = StringUtils.EMPTY;
		try{
			log.info("going to convert object into json {}", pnrEntity);
			result = gson.toJson(pnrEntity);
			log.info("converted json {}", result);
		}catch (Exception e){
			log.error("Exception while converting record to json", e);
		}
		return result;
	}

}