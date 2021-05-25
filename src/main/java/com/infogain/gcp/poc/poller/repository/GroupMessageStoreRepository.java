package com.infogain.gcp.poc.poller.repository;

import java.util.List;
import java.util.Optional;

import com.infogain.gcp.poc.util.RecordStatus;
import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;

import com.infogain.gcp.poc.entity.PNREntity;
import org.springframework.cloud.gcp.data.spanner.repository.query.Query;
import org.springframework.data.repository.query.Param;

import javax.print.attribute.standard.Destination;

public interface GroupMessageStoreRepository extends SpannerRepository<PNREntity, String>{
	
	Optional<List<PNREntity>> findByPnrid(String pnrid);

	PNREntity findByPnridAndMessageseq(String pnrid,String messageseq);

	PNREntity findByPnridAndMessageseqAndDestination(String pnrid,String messageseq, String destination);

	//@Query("SELECT * FROM group_message_store as g WHERE g.parentPnr = ?1 AND g.status NOT IN ?2")
	Optional<List<PNREntity>> findByParentPnrAndStatusIn(String parentPnr, List<Integer> status);

	Optional<List<PNREntity>> findByParentPnrAndStatusInAndDestination(String parentPnr, List<Integer> status, String destination);

}
