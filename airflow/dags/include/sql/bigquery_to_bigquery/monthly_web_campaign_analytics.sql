SELECT
	id,
	session_id,
	sequence_number,
	traffic_source,
	created_at,
	REGEXP_REPLACE(REGEXP_EXTRACT(uri, r'^/[^/]+/?'),r'/$', '') AS uri_source_page
 FROM `alt-dbt-proj.dbt_prod.stg_ecommerce_events`
WHERE
TIMESTAMP_TRUNC(created_at, DAY) BETWEEN "{{ data_interval_start | ds }}" AND "{{ data_interval_end | ds }}" ORDER BY session_id, sequence_number ASC
