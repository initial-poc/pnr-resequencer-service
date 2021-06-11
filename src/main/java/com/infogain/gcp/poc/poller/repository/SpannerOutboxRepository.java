package com.infogain.gcp.poc.poller.repository;

import com.infogain.gcp.poc.poller.entity.OutboxEntity;
import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;

public interface SpannerOutboxRepository extends SpannerRepository<OutboxEntity	, String> { }
