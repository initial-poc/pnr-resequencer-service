package com.infogain.gcp.poc.exception.hanlder;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import lombok.extern.slf4j.Slf4j;

@ControllerAdvice
@Slf4j
public class PNRMessageExceptionHandler {

	@ExceptionHandler(IllegalStateException.class)
	public ResponseEntity<Map<String, String>> handle(IllegalStateException exception){
		log.error("hanlding error of {}",exception.getMessage());
		Map<String, String> response= new HashMap<String, String>();
		
		response.put("errorMessage", exception.getMessage());
		return new ResponseEntity<Map<String,String>>(response, HttpStatus.BAD_REQUEST);
		
	}
	
}
