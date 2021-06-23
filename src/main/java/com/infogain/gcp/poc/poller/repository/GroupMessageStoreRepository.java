package com.infogain.gcp.poc.poller.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;

import com.infogain.gcp.poc.entity.PNREntity;

public interface GroupMessageStoreRepository extends SpannerRepository<PNREntity, String>{
	
	Optional<List<PNREntity>> findByPnrid(String pnrid);
	Optional<List<PNREntity>> findByPnridAndDestination(String pnrid,String destination);

	PNREntity findByPnridAndMessageseq(String pnrid,String messageseq);

	PNREntity findByPnridAndMessageseqAndDestination(String pnrid,String messageseq, String destination);

	//@Query("SELECT * FROM group_message_store as g WHERE g.parentPnr = ?1 AND g.status NOT IN ?2")
	Optional<List<PNREntity>> findByParentPnrAndStatusIn(String parentPnr, List<Integer> status);

	Optional<List<PNREntity>> findByParentPnrAndStatusInAndDestination(String parentPnr, List<Integer> status, String destination);

}
