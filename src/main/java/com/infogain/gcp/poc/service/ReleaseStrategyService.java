package com.infogain.gcp.poc.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.repository.GroupMessageStoreRepository;
import com.infogain.gcp.poc.util.RecordStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReleaseStrategyService {

	private final GroupMessageStoreRepository msgGrpStoreRepository;

	@SuppressWarnings("all")
	public ConnectableFlux<Object> release(PNREntity pnrEntity) {

		return doRelease(pnrEntity);

	}

	private ConnectableFlux<Object> doRelease(PNREntity pnrEntity) {

		Flux<PNREntity> pnrEntityList = msgGrpStoreRepository.findByPnrid(pnrEntity.getPnrid());
		List<PNREntity> pnrList = pnrEntityList.toStream().collect(Collectors.toList());

		ConnectableFlux<Object> flux = Flux.create(fluxSink -> {
			log.info("Getting all the messages from the table by pnr id");

			log.info("Messages are {}", pnrList);

			Map<Integer, Boolean> seqReleasedStatusMap = pnrList.stream()
					.collect(Collectors.toMap(PNREntity::getMessageseq,
							x -> x.getStatus().equals(RecordStatus.RELEASED.getStatusCode()) ? true : false));
			Map<Integer, PNREntity> seqPNREntityMap = pnrList.stream()
					.collect(Collectors.toMap(PNREntity::getMessageseq, x -> x));

			if (Optional.ofNullable(seqReleasedStatusMap.get(1)).isPresent()) {
				seqReleasedStatusMap.put(1, true);
				if (!seqPNREntityMap.get(1).getStatus().equals(RecordStatus.RELEASED.getStatusCode())) {
					fluxSink.next(seqPNREntityMap.get(1));
				}
			}

			log.info("seqReleasedStatusMap {}", seqReleasedStatusMap);
			log.info("seqPNREntityMap {}", seqPNREntityMap);

			pnrList.stream().sorted()
					.filter(x -> Optional.ofNullable(seqReleasedStatusMap.get((x.getMessageseq() - 1))).isPresent())
					.filter(x -> seqReleasedStatusMap.get(x.getMessageseq() - 1)).filter(x -> !seqPNREntityMap
							.get(x.getMessageseq()).getStatus().equals(RecordStatus.RELEASED.getStatusCode()))
					.forEach(x -> {
						seqReleasedStatusMap.put(x.getMessageseq(), true);
						fluxSink.next(x);
					});

		}).publish();
		return flux;
	}

}
