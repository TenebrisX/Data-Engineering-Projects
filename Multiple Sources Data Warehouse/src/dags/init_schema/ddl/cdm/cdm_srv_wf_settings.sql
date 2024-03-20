create table if not exists cdm.srv_wf_settings (
	id serial4 not null constraint srv_wf_settings_pk primary key,
	workflow_key varchar not null,
	workflow_settings json not null,
	constraint cdm_srv_wf_settings_workflow_key_unq unique (workflow_key)
);