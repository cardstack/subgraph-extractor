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
)