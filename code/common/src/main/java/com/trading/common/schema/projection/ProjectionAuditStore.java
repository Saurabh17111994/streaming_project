package com.trading.common.schema.projection;

import java.util.List;

/** Durable immutable Execution_Audit LOG writer for T6 projections. */
public interface ProjectionAuditStore {

    void append(ProjectionAuditRecord record) throws Exception;

    List<ProjectionAuditRecord> all() throws Exception;
}
