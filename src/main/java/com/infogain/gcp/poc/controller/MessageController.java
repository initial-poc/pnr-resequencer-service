package com.infogain.gcp.poc.controller;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
	public ResponseEntity<String> processMessge(@RequestBody PNRModel message) {
		log.info("Got the message {}",message);
		try {
			pnrSequencingService.processPNR(message);
        	return new ResponseEntity<>("Success", HttpStatus.OK);
        } catch(DataIntegrityViolationException d) {
        	return new ResponseEntity<>("Duplicate", HttpStatus.BAD_REQUEST);
        } catch(Exception e) {
        	return new ResponseEntity<>("Failure", HttpStatus.BAD_REQUEST);
        }
	}
}
