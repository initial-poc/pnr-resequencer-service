package com.sabre.ngp.ar.etfinalizationservice.repository;

import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;

public interface SpannerOutboxRepository extends SpannerRepository<OutboxEntity, String> { }
