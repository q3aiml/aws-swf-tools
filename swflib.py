"""
Misc AWS SWF helper functions.
"""

import sys

import boto.swf.layer2 as swf2


def get_all_events(conn, domain_name, run_id, workflow_id):
    """
    Return all events of an execution (handles pagination, returning all pages).
    """
    next_page_token = None
    events = []
    while True:
        history_page = conn.get_workflow_execution_history(domain_name, run_id, workflow_id,
                                                           next_page_token=next_page_token)
        events.extend(history_page['events'])

        if 'nextPageToken' in history_page:
            next_page_token = history_page['nextPageToken']
        else:
            break
    return events


def summarize_events(events, raise_on_unknown=False, verbose=False):
    """
    Groups events relating to the same activity task, child workflow execution, etc.
    Returns a dict of event id to the aggregated event info.

    Args:
        events (string): All events to aggregate. It is assumed they are in ascending
            event id order as returned by :py:func:`get_all_events`

    Aggregate events include optional properties:

    * 'time_schedule': time of the schedule event
    * 'time_start': time of the start event
    * 'time_end': time of the end event
    * 'type': 'child_workflow' or 'activity'
    * as well as all properties from each individual event combined. If a property
      exists in multiple events that are combined in a single aggregated event, the
      property of the later event (based on order provided in the events parameter) is used.
    """
    aggregate_events = {}
    for event in events:
        related_id = None
        attrs = {}
        foo = {}

        if 'activityTaskScheduledEventAttributes' in event:
            attrs = event['activityTaskScheduledEventAttributes']
            foo['time_schedule'] = event['eventTimestamp']
            foo['type'] = 'activity'
            related_id = event['eventId']

        elif 'activityTaskStartedEventAttributes' in event:
            attrs = event['activityTaskStartedEventAttributes']
            foo['time_start'] = event['eventTimestamp']
            related_id = attrs['scheduledEventId']

        elif 'activityTaskCompletedEventAttributes' in event:
            attrs = event['activityTaskCompletedEventAttributes']
            foo['time_end'] = event['eventTimestamp']
            related_id = attrs['scheduledEventId']

        elif 'activityTaskFailedEventAttributes' in event:
            attrs = event['activityTaskFailedEventAttributes']
            foo['time_end'] = event['eventTimestamp']
            related_id = attrs['scheduledEventId']

        elif 'activityTaskCanceledEventAttributes' in event:
            attrs = event['activityTaskCanceledEventAttributes']
            foo['time_end'] = event['eventTimestamp']
            related_id = attrs['scheduledEventId']

        elif 'startChildWorkflowExecutionInitiatedEventAttributes' in event:
            attrs = event['startChildWorkflowExecutionInitiatedEventAttributes']
            foo['time_scheduled'] = event['eventTimestamp']
            foo['type'] = 'child_workflow'
            related_id = event['eventId']

        elif 'childWorkflowExecutionStartedEventAttributes' in event:
            attrs = event['childWorkflowExecutionStartedEventAttributes']
            foo['time_start'] = event['eventTimestamp']
            related_id = attrs['initiatedEventId']

        elif 'childWorkflowExecutionCompletedEventAttributes' in event:
            attrs = event['childWorkflowExecutionCompletedEventAttributes']
            foo['time_end'] = event['eventTimestamp']
            related_id = attrs['initiatedEventId']

        elif 'childWorkflowExecutionFailedEventAttributes' in event:
            attrs = event['childWorkflowExecutionFailedEventAttributes']
            foo['time_end'] = event['eventTimestamp']
            related_id = attrs['initiatedEventId']

        else:
            if raise_on_unknown:
                raise Exception("unsupported " + event)
            elif verbose:
                print("unsupported event " + str(event), file=sys.stderr)

        if related_id:
            aggregate_event = aggregate_events.get(related_id)
            if not aggregate_event:
                aggregate_event = {'attributes': {}}
                aggregate_events[related_id] = aggregate_event
            aggregate_event['attributes'].update(attrs)
            aggregate_event.update(foo)
        elif verbose:
            print("no related id!", file=sys.stderr)

    return aggregate_events

