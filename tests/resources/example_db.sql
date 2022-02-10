create schema sgd1;
create schema subgraphs;

CREATE TABLE deployment_schemas (
    subgraph    varchar,
    name        varchar,
    network     varchar,
    active boolean
);

insert into deployment_schemas values (
    'SUBGRAPHIPFS',
    'sgd1',
    'poa-sokol',
    TRUE
);

CREATE TABLE subgraphs.subgraph_version (
    deployment    varchar,
    id varchar
);

insert into subgraphs.subgraph_version values (
    'SUBGRAPHIPFS',
    'internalversion1'
);

CREATE TABLE subgraphs.subgraph (
    name                varchar,
    current_version     varchar
);

insert into subgraphs.subgraph values (
    'my_test_subgraph',
    'internalversion1'
);

CREATE TABLE sgd1.sample_table (
    block_number numeric,
    amount numeric,
    from_address text,
    to_address text
);

CREATE TABLE subgraphs.subgraph_deployment (
  firehose_cursor text NULL,
  id integer NOT NULL,
  last_healthy_ethereum_block_number numeric NULL,
  last_healthy_ethereum_block_hash bytea NULL,
  max_reorg_depth integer NOT NULL,
  current_reorg_depth integer NOT NULL,
  reorg_count integer NOT NULL,
  health text NOT NULL, -- this is actually another type but is unused in the tests
  non_fatal_errors text [ ] NULL,
  fatal_error text NULL,
  graft_block_number numeric NULL,
  graft_block_hash bytea NULL,
  graft_base text NULL,
  entity_count numeric NOT NULL,
  latest_ethereum_block_number numeric NULL,
  latest_ethereum_block_hash bytea NULL,
  earliest_ethereum_block_number numeric NULL,
  earliest_ethereum_block_hash bytea NULL,
  synced boolean NOT NULL,
  failed boolean NOT NULL,
  deployment text NOT NULL
);

insert into subgraphs.subgraph_deployment values (
    '',
    1,
    NULL,
    NULL,
    0,
    0,
    0,
    'healthy',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    0,
    10000000,
    NULL,
    1,
    NULL,
    TRUE,
    FALSE,
    'SUBGRAPHIPFS'
);