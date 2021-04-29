package com.infogain.gcp.poc.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.domainmodel.ServiceResponse;
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
	public ResponseEntity<ServiceResponse> processMessge(@RequestBody PNRModel message) {
		log.info("Got the message in controller {}", message);
		
		return new ResponseEntity<ServiceResponse>(new ServiceResponse(pnrSequencingService.processPNR(message)), HttpStatus.CREATED);
	}
}
