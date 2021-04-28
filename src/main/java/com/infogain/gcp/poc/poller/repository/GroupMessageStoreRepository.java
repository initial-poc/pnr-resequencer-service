package com.infogain.gcp.poc.poller.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;

import com.infogain.gcp.poc.entity.PNREntity;

public interface GroupMessageStoreRepository extends SpannerRepository<PNREntity	, String>{
	
	Optional<List<PNREntity>> findByPnrid(String pnrid);

}
