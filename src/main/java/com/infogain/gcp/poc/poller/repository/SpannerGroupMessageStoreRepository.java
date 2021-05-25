package com.infogain.gcp.poc.poller.repository;

import com.infogain.gcp.poc.poller.entity.GroupMessageStoreEntity;
import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;

public interface SpannerGroupMessageStoreRepository extends SpannerRepository<GroupMessageStoreEntity, String> { }
