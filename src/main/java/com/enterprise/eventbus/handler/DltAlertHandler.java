package com.enterprise.eventbus.handler;

import com.enterprise.eventbus.model.DltRecord;

/**
 * Optional callback invoked when DLT strategy is ALERT_AND_STORE.
 * Use to send PagerDuty alerts, Slack notifications, create JIRA tickets, etc.
 */
@FunctionalInterface
public interface DltAlertHandler {

    void onDeadLetter(DltRecord record);
}
