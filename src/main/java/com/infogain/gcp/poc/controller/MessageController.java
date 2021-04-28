package com.infogain.gcp.poc.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.service.PNRSequencingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("api")
@RequiredArgsConstructor
@Slf4j
public class MessageController {

	private final PNRSequencingService pnrSequencingService;
	
	@PostMapping("/pnrs")
	public String processMessge(@RequestBody PNRModel message) {
		log.info("Got the message {}",message);
		 
		pnrSequencingService.processPNR(message);
		
		return "success";
		
	}
}
