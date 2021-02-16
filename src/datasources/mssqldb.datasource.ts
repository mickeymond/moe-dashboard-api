import {inject, lifeCycleObserver, LifeCycleObserver} from '@loopback/core';
import {juggler} from '@loopback/repository';
import dotenv from 'dotenv';

dotenv.config();

const config = {
  name: 'mssqldb',
  connector: 'mssql',
  url: process.env.MSSQL_URI,
};

// Observe application's life cycle to disconnect the datasource when
// application is stopped. This allows the application to be shut down
// gracefully. The `stop()` method is inherited from `juggler.DataSource`.
// Learn more at https://loopback.io/doc/en/lb4/Life-cycle.html
@lifeCycleObserver('datasource')
export class MssqldbDataSource extends juggler.DataSource
  implements LifeCycleObserver {
  static dataSourceName = 'mssqldb';
  static readonly defaultConfig = config;

  constructor(
    @inject('datasources.config.mssqldb', {optional: true})
    dsConfig: object = config,
  ) {
    super(dsConfig);
  }
}
