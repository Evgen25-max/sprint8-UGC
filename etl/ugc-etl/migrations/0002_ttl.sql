ALTER TABLE __DB__.clicks        MODIFY TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;
ALTER TABLE __DB__.page_views    MODIFY TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;
ALTER TABLE __DB__.video_events  MODIFY TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;
ALTER TABLE __DB__.search_events MODIFY TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;
