package com.sabre.ngp.ar.etfinalizationservice.component;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.sabre.ngp.ar.etfinalizationservice.entity.PNREntity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MessageConverter {

	@Autowired
	private Gson gson;

	public String convert(PNREntity pnrEntity) {
		String result = StringUtils.EMPTY;
		try{
			result = gson.toJson(pnrEntity);
		}catch (Exception e){
			log.error("Exception while converting record to json {}", e);
		}
		return result;
	}

}