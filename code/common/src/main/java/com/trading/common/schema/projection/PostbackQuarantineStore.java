package com.trading.common.schema.projection;

import java.util.List;

/** Durable immutable Postback_Quarantine LOG writer (16_postback_quarantine.sql). */
public interface PostbackQuarantineStore {

    void append(QuarantinedPostback row) throws Exception;

    List<QuarantinedPostback> all() throws Exception;
}
