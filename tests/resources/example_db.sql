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

CREATE TABLE sgd1.prepaid_card_ask_sample (
  block_range int4range NOT NULL,
  vid bigint NOT NULL,
  ask_price numeric NOT NULL,
  issuing_token text NOT NULL,
  sku text NOT NULL,
  id text NOT NULL
);

ALTER TABLE sgd1.prepaid_card_ask_sample ADD CONSTRAINT prepaid_card_ask_pkey PRIMARY KEY (vid);
insert into "sgd1"."prepaid_card_ask_sample" ("ask_price", "block_range", "id", "issuing_token", "sku", "vid") values ('10000000000000000000', '[18460372,)', '0x01974608d9b1b9b7dc822715f7167437d3fa938b579ccc2f60b135d01d35f505', '0x26F2319Fbb44772e0ED58fB7c99cf8da59e2b5BE', '0x01974608d9b1b9b7dc822715f7167437d3fa938b579ccc2f60b135d01d35f505', '1');
insert into "sgd1"."prepaid_card_ask_sample" ("ask_price", "block_range", "id", "issuing_token", "sku", "vid") values ('10000000000000000000', '[18887449,)', '0x2a635070ac5332e28e657bfd717363165dba145ddc6d5eca39b12faacb861149', '0x26F2319Fbb44772e0ED58fB7c99cf8da59e2b5BE', '0x2a635070ac5332e28e657bfd717363165dba145ddc6d5eca39b12faacb861149', '2');
insert into "sgd1"."prepaid_card_ask_sample" ("ask_price", "block_range", "id", "issuing_token", "sku", "vid") values ('25000000000000000000', '[18888080,)', '0xe48b3fa88fea42a5720fdeeaabf99065c666be73ff9b99977b403521deb47eee', '0x26F2319Fbb44772e0ED58fB7c99cf8da59e2b5BE', '0xe48b3fa88fea42a5720fdeeaabf99065c666be73ff9b99977b403521deb47eee', '3');
insert into "sgd1"."prepaid_card_ask_sample" ("ask_price", "block_range", "id", "issuing_token", "sku", "vid") values ('75000000000000000000', '[18888096,)', '0x72f15a7e3b61878976a917626f019b4488ca290f868b37a321e84fade75735d5', '0x26F2319Fbb44772e0ED58fB7c99cf8da59e2b5BE', '0x72f15a7e3b61878976a917626f019b4488ca290f868b37a321e84fade75735d5', '4');
insert into "sgd1"."prepaid_card_ask_sample" ("ask_price", "block_range", "id", "issuing_token", "sku", "vid") values ('150000000000000000000', '[18888106,)', '0xc1f1cb58f15cf68dd8a11b3e6189be3a79bbf10d4c826f9323c302b9a1a8236c', '0x26F2319Fbb44772e0ED58fB7c99cf8da59e2b5BE', '0xc1f1cb58f15cf68dd8a11b3e6189be3a79bbf10d4c826f9323c302b9a1a8236c', '5');
insert into "sgd1"."prepaid_card_ask_sample" ("ask_price", "block_range", "id", "issuing_token", "sku", "vid") values ('250000000000000000000', '[18888119,)', '0x73ea1c11dbd7fd47fd0eb753013173dd5cbe04f4d11de3b96d471ae7a6dc7e22', '0x26F2319Fbb44772e0ED58fB7c99cf8da59e2b5BE', '0x73ea1c11dbd7fd47fd0eb753013173dd5cbe04f4d11de3b96d471ae7a6dc7e22', '6');
;


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
    19000000,
    NULL,
    18000000,
    NULL,
    TRUE,
    FALSE,
    'SUBGRAPHIPFS'
);