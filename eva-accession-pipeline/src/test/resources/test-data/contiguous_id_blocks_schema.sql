CREATE TABLE contiguous_id_blocks (
  id bigint not NULL,
  application_instance_id varchar(255) not NULL,
  category_id varchar(255) not NULL,
  first_value bigint not NULL,
  last_committed bigint not NULL,
  last_value bigint not NULL,
  reserved boolean NOT NULL,
  created_timestamp timestamp NOT NULL
);
-- TODO update created_timestamp to last_updated_timestamp after accessions commons PR Merge
