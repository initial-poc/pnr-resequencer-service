package com.infogain.gcp.poc.service;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.repository.GroupMessageStoreRepository;
import com.infogain.gcp.poc.util.RecordStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReleaseStrategyService {

    private final GroupMessageStoreRepository msgGrpStoreRepository;

    @SuppressWarnings("all")
    public Map<String,List<PNREntity>> release(PNREntity pnrEntity) {
        log.info("Getting all the messages from the table by pnr id");

        Optional<List<PNREntity>> pnrEntityList = msgGrpStoreRepository.findByPnrid(pnrEntity.getPnrid());

        log.info("list data : {}", pnrEntityList);

        List<Integer> numList = new ArrayList<>();

        //This indicates - childs are still getting processed
        numList.add(0);
        numList.add(1);
        numList.add(2);
        numList.add(4);

        Optional<List<PNREntity>> pnrEntityChildren =
                msgGrpStoreRepository.findByParentPnrAndStatusInAndDestination(pnrEntity.getPnrid(), numList,pnrEntity.getDestination());
                //msgGrpStoreRepository.findByParentPnrAndStatusIn(pnrEntity.getPnrid(),numList);

        Map<String,List<PNREntity>> returnMap = new HashMap<String,List<PNREntity>>();
        if(pnrEntityChildren.isPresent() && pnrEntityChildren.get().isEmpty()) {
        List<PNREntity> pnrListComplete = pnrEntityList.get();

        Map<String,List<PNREntity>> pnrMap =
                pnrListComplete.stream().collect(Collectors.groupingBy(x->x.getDestination()));

        log.info("pnrMap values : {}", pnrMap);

        pnrMap.keySet().stream().forEach( key-> {
            List <PNREntity> pnrList = pnrMap.get(key);
            List<PNREntity> returnList = new ArrayList<PNREntity>();
            log.info("Messages are {}", pnrList);
            Map<Integer, Boolean> seqReleasedStatusMap = pnrList.stream()
                    .collect(Collectors.toMap(PNREntity::getMessageseq, x -> x.getStatus().equals(RecordStatus.RELEASED.getStatusCode()) ? true : false));
            Map<Integer, PNREntity> seqPNREntityMap = pnrList.stream()
                    .collect(Collectors.toMap(PNREntity::getMessageseq, x -> x));

            if (Optional.ofNullable(seqReleasedStatusMap.get(1)).isPresent()) {
                seqReleasedStatusMap.put(1, true);
                if (!(seqPNREntityMap.get(1).getStatus().equals(RecordStatus.COMPLETED.getStatusCode()) ||
                        seqPNREntityMap.get(1).getStatus().equals(RecordStatus.RELEASED.getStatusCode()))) {
                    returnList.add(seqPNREntityMap.get(1));
                }
            }

            log.info("seqReleasedStatusMap {}", seqReleasedStatusMap);
            log.info("seqPNREntityMap {}", seqPNREntityMap);

            pnrList.stream().sorted().
                    filter(x -> Optional.ofNullable(seqReleasedStatusMap.get((x.getMessageseq() - 1))).isPresent()).
                    filter(x -> seqReleasedStatusMap.get(x.getMessageseq() - 1))
                    //filter(x -> !seqPNREntityMap.get(x.getMessageseq()).getStatus().equals(RecordStatus.RELEASED.getStatusCode()))
                    .forEach(x -> {
                        seqReleasedStatusMap.put(x.getMessageseq(), true);
                        if (!(x.getStatus().equals(RecordStatus.RELEASED.getStatusCode()) || x.getStatus().equals(RecordStatus.COMPLETED.getStatusCode()))) {
                            returnList.add(x);
                        }
                    });
            returnMap.put(key,returnList);
        });
        log.info("returning the list {}", returnMap);
        return returnMap;
        } else {
            //null will only be returned when current record is PARENT.
            return null;
        }
    }

    public static Map<String,List<PNREntity>>  releaseTest(List<PNREntity> pnrListTest) {
        log.info("Getting all the messages from the table by pnr id");
        //Optional<List<PNREntity>> pnrEntityList = msgGrpStoreRepository.findByPnrid(pnrEntity.getPnrid());
        Map<String,List<PNREntity>> returnMap = new HashMap<String,List<PNREntity>>();

        List<PNREntity> pnrListComplete = pnrListTest;

        Map<String,List<PNREntity>> pnrMap =
                pnrListComplete.stream().collect(Collectors.groupingBy(x->x.getDestination()));

        pnrMap.keySet().stream().forEach( key-> {
            List <PNREntity> pnrList = pnrMap.get(key);
            List<PNREntity> returnList = new ArrayList<PNREntity>();
            log.info("Messages are {}", pnrList);
            Map<Integer, Boolean> seqReleasedStatusMap = pnrList.stream()
                    .collect(Collectors.toMap(PNREntity::getMessageseq, x -> x.getStatus().equals(RecordStatus.RELEASED.getStatusCode()) ? true : false));
            Map<Integer, PNREntity> seqPNREntityMap = pnrList.stream()
                    .collect(Collectors.toMap(PNREntity::getMessageseq, x -> x));

            if (Optional.ofNullable(seqReleasedStatusMap.get(1)).isPresent()) {
                seqReleasedStatusMap.put(1, true);
                if (!(seqPNREntityMap.get(1).getStatus().equals(RecordStatus.COMPLETED.getStatusCode()) ||
                        seqPNREntityMap.get(1).getStatus().equals(RecordStatus.RELEASED.getStatusCode()))) {
                    returnList.add(seqPNREntityMap.get(1));
                }
            }

            log.info("seqReleasedStatusMap {}", seqReleasedStatusMap);
            log.info("seqPNREntityMap {}", seqPNREntityMap);

            pnrList.stream().sorted().
                    filter(x -> Optional.ofNullable(seqReleasedStatusMap.get((x.getMessageseq() - 1))).isPresent()).
                    filter(x -> seqReleasedStatusMap.get(x.getMessageseq() - 1))
                    //filter(x -> !seqPNREntityMap.get(x.getMessageseq()).getStatus().equals(RecordStatus.RELEASED.getStatusCode()))
                    .forEach(x -> {
                        seqReleasedStatusMap.put(x.getMessageseq(), true);
                        if (!(x.getStatus().equals(RecordStatus.RELEASED.getStatusCode()) || x.getStatus().equals(RecordStatus.COMPLETED.getStatusCode()))) {
                            returnList.add(x);
                        }
                    });
            returnMap.put(key,returnList);
        });
        log.info("returning the list {}", returnMap);
        return returnMap;
    }

    public static void main(String[] args) {
        List<PNREntity> returnList = new ArrayList<PNREntity>();
        returnList.add(PNREntity.builder().destination("x").pnrid("PNR123").messageseq(1).status(RecordStatus.RELEASED.getStatusCode()).build());
        returnList.add(PNREntity.builder().destination("x").pnrid("PNR123").messageseq(3).status(RecordStatus.IN_PROGRESS.getStatusCode()).build());
		returnList.add(PNREntity.builder().destination("x").pnrid("PNR123").messageseq(2).status(RecordStatus.RELEASED.getStatusCode()).build());
		returnList.add(PNREntity.builder().destination("x").pnrid("PNR123").messageseq(5).status(RecordStatus.IN_PROGRESS.getStatusCode()).build());

        returnList.add(PNREntity.builder().destination("y").pnrid("PNR123").messageseq(1).status(RecordStatus.RELEASED.getStatusCode()).build());
        returnList.add(PNREntity.builder().destination("y").pnrid("PNR123").messageseq(3).status(RecordStatus.IN_PROGRESS.getStatusCode()).build());
        returnList.add(PNREntity.builder().destination("y").pnrid("PNR123").messageseq(2).status(RecordStatus.RELEASED.getStatusCode()).build());
        returnList.add(PNREntity.builder().destination("y").pnrid("PNR123").messageseq(5).status(RecordStatus.IN_PROGRESS.getStatusCode()).build());

        releaseTest(returnList);

    }
}
