CREATE TABLE IF NOT EXISTS dds.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR NOT NULL UNIQUE,
    workflow_settings text NOT NULL
);