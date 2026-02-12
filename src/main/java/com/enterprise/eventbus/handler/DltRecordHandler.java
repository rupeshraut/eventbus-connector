package com.enterprise.eventbus.handler;

import com.enterprise.eventbus.model.DltRecord;

/**
 * Custom DLT processing handler invoked when DLT strategy is CUSTOM.
 */
@FunctionalInterface
public interface DltRecordHandler {

    void handle(DltRecord record);
}
