# Stinkerpg

### A Postgresql management tool.

A command line tool to ease development and administration.

### FAQ



The `sync` command is different entirely, `sync` runs a hashing function on the tables in both databases and only modifies the data that is different, saving tons and bandwidth and time.



### Installation.

Required nodejs v6+

`npm install -g stinkerpg`

## Documentation


### Synchronize two Postgresql databases.

`stinkerpg sync` Synchronizes tables, indexes and data from the source database to the target database. The target database is modified to match the source.

```bash
Stinkerpg Sync
==============================

Sync two Postgresql databases.

Usage:
  stinkerpg sync [options]
  stinkerpg sync --sh host[:port] --th host[:port] --sd dbName --td dbName
  stinkerpg sync -h | --help

Options:
  --sh, --sourceHost=<host[:port]>    Source host, defaults to 'localhost:21015'
  --th, --targetHost=<host[:port]>    Target host, defaults to 'localhost:21015'
  --sd, --sourceDB=<dbName>           Source database
  --td, --targetDB=<dbName>           Target database

  --pt, --pickTables=<table1,table2>  Comma separated list of tables to sync (whitelist)
  --ot, --omitTables=<table1,table2>  Comma separated list of tables to ignore (blacklist)
                                      Note: '--pt' and '--ot' are mutually exclusive options.

  --user                              Source and Target username
  --password                          Source and Target password

  --su                                Source username, overrides --user
  --sp                                Source password, overrides --password

  --tu                                Target username, overrides --user
  --tp                                Target password, overrides --password
```
