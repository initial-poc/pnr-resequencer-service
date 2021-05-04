package com.infogain.gcp.poc.poller.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.infogain.gcp.poc.entity.PNREntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface GroupMessageStoreRepository extends ReactiveCrudRepository<PNREntity	, String>{
	Flux<PNREntity> findByPnrid(String pnrid);

	Mono<PNREntity> findByPnridAndMessageseq(String pnrid,String messageseq);

}
