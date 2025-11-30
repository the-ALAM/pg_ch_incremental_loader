# milestones

- [x] docker-compose for the pg, ch, spark instances
- [x] configure spark
- [x] DDL scripts
- [x] schema def pg
- [x] schema def ch
- [x] populate pg
- [x] connect spk-pg-ch
- [x] JDBC drivers for pg+ch
- [-] checkpoint storage in pg
- [x] incremental load records
- [x] scheduler
- [x] batch option
  - [x] created_at
  - [ ] xmin pg column
  - [ ] feat_flags
- [x] validation
- [ ] use apache/spark rather than bitnami's and create a configured bare boned setup
- [x] inject the DDLs and data on-start
- [-] retry and idempotency
- [ ] add faker and generate additional synth data
- [ ] streaming option
  - [ ] cdc
- [ ] the FDW approach to loading data
- [ ] the peerdb approach to loading data
- [ ] create the DDL for the logs table and stream it
- [ ] index on created_at, updated_at metadata columns
****