package com.infogain.gcp.poc.service;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.repository.GroupMessageStoreRepository;
import com.infogain.gcp.poc.util.GroupMessageRecordStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReleaseStrategyService {

    private final GroupMessageStoreRepository msgGrpStoreRepository;

    public List<PNREntity> release(PNREntity pnrEntity) {

       Optional<List<PNREntity>> pnrEntityList = msgGrpStoreRepository.findByPnrid(pnrEntity.getPnrid());
    //    Optional<List<PNREntity>> pnrEntityList = msgGrpStoreRepository.findByPnridAndDestination(pnrEntity.getPnrid(),pnrEntity.getDestination());
        List<Integer> status = new ArrayList<>();

        //This indicates - childs are still getting processed
        status.add(0);
        status.add(1);
        status.add(2);
        status.add(4);

        Optional<List<PNREntity>> pnrEntityChildren =
               // msgGrpStoreRepository.findByParentPnrAndStatusInAndDestination(pnrEntity.getPnrid(), numList,pnrEntity.getDestination());
                msgGrpStoreRepository.findByParentPnrAndStatusIn(pnrEntity.getPnrid(),status);

        Map<String,List<PNREntity>> returnMap = new HashMap<String,List<PNREntity>>();
        if(pnrEntityChildren.isPresent() && pnrEntityChildren.get().isEmpty()) {
            List <PNREntity> pnrList = pnrEntityList.get();
            List<PNREntity> returnList = new ArrayList<PNREntity>();

          //  pnrList.stream().collect(Collectors.toMap(pnrEntity1 -> pnrEntity1.getMessageseq(),o -> o,(pnrEntity1, pnrEntity2) -> pnrEntity1));

            Map<Integer, Boolean> seqReleasedStatusMap= Maps.newHashMap();
            Map<Integer, PNREntity> seqPNREntityMap=Maps.newHashMap();
            for(PNREntity entity: pnrList){
                seqReleasedStatusMap.put(entity.getMessageseq(),isProcessed(entity));
                seqPNREntityMap.put(entity.getMessageseq(),entity);
            }

            if (Optional.ofNullable(seqReleasedStatusMap.get(1)).isPresent()) {
                seqReleasedStatusMap.put(1, true);
                if (!(seqPNREntityMap.get(1).getStatus().equals(GroupMessageRecordStatus.COMPLETED.getStatusCode()) ||
                        seqPNREntityMap.get(1).getStatus().equals(GroupMessageRecordStatus.RELEASED.getStatusCode()))) {
                    returnList.add(seqPNREntityMap.get(1));
                }
            }

            pnrList.stream().sorted().
                    filter(x -> Optional.ofNullable(seqReleasedStatusMap.get((x.getMessageseq() - 1))).isPresent()).
                    filter(x -> seqReleasedStatusMap.get(x.getMessageseq() - 1))
                    //filter(x -> !seqPNREntityMap.get(x.getMessageseq()).getStatus().equals(RecordStatus.RELEASED.getStatusCode()))
                    .forEach(x -> {
                        seqReleasedStatusMap.put(x.getMessageseq(), true);
                        if (!(x.getStatus().equals(GroupMessageRecordStatus.RELEASED.getStatusCode()) || x.getStatus().equals(GroupMessageRecordStatus.COMPLETED.getStatusCode()))) {
                            returnList.add(x);
                        }
                    });

        return returnList;
        } else {
            //null will only be returned when current record is PARENT.
            return null;
        }
    }

private boolean isProcessed(PNREntity pnrEntity){
        boolean result=false;
    if ((pnrEntity.getStatus().equals(GroupMessageRecordStatus.RELEASED.getStatusCode()) || pnrEntity.getStatus().equals(GroupMessageRecordStatus.COMPLETED.getStatusCode()))) {
      result=true;
    }
    return result;
}
}
