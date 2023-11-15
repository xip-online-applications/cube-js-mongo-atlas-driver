/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `MongoAtlasJDBC` and related types declaration.
 */

import {
    getEnv,
    assertDataSource, CancelablePromise,
} from '@cubejs-backend/shared';
import fs from 'fs';
import path from 'path';
import * as SqlString from 'sqlstring';
import {
    TableStructure,
    DownloadQueryResultsOptions,
    DownloadQueryResultsResult,
} from '@cubejs-backend/base-driver';
import {MongoAtlasQuery} from './MongoAtlasQuery';
import {promisify} from "util";
import {SchemaStructure} from "@cubejs-backend/base-driver/dist/src/driver.interface";
import { v4 as uuidv4 } from "uuid";

import { Readable } from 'stream';
import { BaseDriver } from '@cubejs-backend/base-driver';
import genericPool, { Factory, Pool } from 'generic-pool';
import {DriverConfiguration} from "./types";

export type MongoAtlasConfiguration = DriverConfiguration;

const DriverManager = require('@cubejs-backend/jdbc/lib/drivermanager');
const Connection = require('@cubejs-backend/jdbc/lib/connection');
const { MongoClient } = require('mongodb');

ResultSetMetaData.prototype.getColumnTypeName = function (column: any, callback: any) {
    this._rsmd.getColumnTypeName(column, function (err: any, typeMame: any) {
        try {
            if (err) {
                return callback(err);
            } else {
                return callback(null, typeMame);
            }
        } catch (err) {
            return callback(err);
        }
    });
}

const applyParams = (query: string, params: object | any[]) => SqlString.format(query, params);

// promisify Connection methods
Connection.prototype.getMetaDataAsync = promisify(Connection.prototype.getMetaData);
// promisify DatabaseMetaData methods
DatabaseMetaData.prototype.getSchemasAsync = promisify(DatabaseMetaData.prototype.getSchemas);
DatabaseMetaData.prototype.getTablesAsync = promisify(DatabaseMetaData.prototype.getTables);

interface ExtendedPool extends Pool<any> {
    _factory: Factory<any>;
}

export class MongoAtlasDriver extends BaseDriver {
    private _id: string;

    protected readonly config: DriverConfiguration;

    protected pool: ExtendedPool;

    protected props: any;

    public constructor(
        config: Partial<DriverConfiguration> & {
            /**
             * Data source name.
             */
            dataSource?: string,

            /**
             * Max pool size value for the [cube]<-->[db] pool.
             */
            maxPoolSize?: number,

            /**
             * Time to wait for a response from a connection after validation
             * request before determining it as not valid. Default - 60000 ms.
             */
            testConnectionTimeout?: number,
        } = {}
    ) {
        super({
            testConnectionTimeout: config.testConnectionTimeout || 60000,
        });

        const dataSource =
            config.dataSource ||
            assertDataSource('default');

        const url: string =
            config?.url ||
            getEnv('jdbcUrl', {dataSource});

        const { poolOptions, ...dbOptions } = config;

        this.config = {
            dbType: 'mongo-atlas',
            url: url,
            properties: {
                user: process.env.CUBEJS_DB_USER,
                password: process.env.CUBEJS_DB_PASS,
                port: process.env.CUBEJS_DB_PORT
            },
            ...dbOptions
        } as DriverConfiguration;

        if (!this.config.url) {
            throw new Error('url is required property');
        }

        console.log('creating pool ....');
        this.pool = genericPool.createPool({
            create: async () => {
                try {
                    console.log('creating connection ...');
                    const connection = new MongoClient(this.config.url);
                    await connection.connect();

                    console.log('Connection created successfully', { connectionId: connection.id });
                    return connection;

                } catch (error: any) {

                    console.log('Connection creation failed', { error: error.message });
                    throw error; // Ensure the error is not swallowed
                }
            },
            destroy: async (connection) => {
                try {
                    console.log('destroying connection ...');
                    await connection.close();

                    console.log('Connection closed successfully', { connectionId: connection.id });
                } catch (error: any) {
                    console.log('Connection could not be closed', { connectionId: connection.id, error: error.message });
                }
            },
            validate: async (connection) => (
                new Promise((resolve) => {
                    console.log('validating connection ...');

                    const database = connection.db(this.config.database);
                    database.command({ ping: 1 }).then(
                        () => {
                            console.log('Connection validation succeeded', { connectionId: connection.id });
                            resolve(true);
                        },
                        (err: Error) => {
                            console.log('Connection validation failed', { connectionId: connection.id, error: err.message });
                            resolve(false);
                        }
                    ).catch((e: Error) => {
                        console.log('Connection validation error', { connectionId: connection.id, error: e.stack || e.message });
                        this.databasePoolError(e);
                        resolve(false);
                    });
                })
            )
        }, {
            min: 0,
            max: config.maxPoolSize || getEnv('dbMaxPoolSize', { dataSource }) || 3,
            evictionRunIntervalMillis: 10000,
            softIdleTimeoutMillis: 30000,
            idleTimeoutMillis: 30000,
            testOnBorrow: true,
            acquireTimeoutMillis: 120000,
            ...(poolOptions || {})
        }) as ExtendedPool;

        this._id = uuidv4();

        console.log(this._id, 'creating driver for: ' + JSON.stringify(config));
    }

    public static dialectClass() {
        return MongoAtlasQuery;
    }

    public override readOnly() {
        return true;
    }

    public async testConnection() {
        let err;
        let connection;
        try {
            connection = await this.pool._factory.create();
        } catch (e: any) {
            err = e.message || e;
        }
        if (err) {
            throw new Error(err.toString());
        } else {
            await this.pool._factory.destroy(connection);
        }
    }

    public async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
        try {
            const queryWithParams = applyParams(query, values);

            const conn = await this.pool.acquire();

            const database = conn.db(this.config.database);

            const cancelObj: {cancel?: Function} = {};

            const resultSet = (await this.queryPromised(queryWithParams, cancelObj, {} )).toArray()

            const transformedArray = resultSet.map(item => {
                const key = Object.keys(item)[0]; // Gets the first key of the object
                return item[key]; // Returns the value of this key
            });

            const rows: R[] = transformedArray;

        }

        catch (error) {
            // Handle any errors that occurred during processing, including cleanup if necessary
            console.error("An error occurred during the query operation:", error);
            // Depending on how queryRaw works, you might need to do additional cleanup here.
            throw error; // Re-throw the error after any necessary cleanup
        }
    }

    // private async queryRaw<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
    //     const queryWithParams = applyParams(query, values);
    //     const cancelObj: {cancel?: Function} = {};
    //     const promise = this.queryPromised(queryWithParams, cancelObj, this.prepareConnectionQueries());
    //     (promise as CancelablePromise<any>).cancel =
    //         () => cancelObj.cancel && cancelObj.cancel() ||
    //             Promise.reject(new Error('Statement is not ready'));
    //     return promise;
    // }
    //
    // protected async withConnection<T extends Function>(fn: T) {
    //     const conn = await this.pool.acquire();
    //
    //     try {
    //         return await fn(conn);
    //     } finally {
    //         await this.pool.release(conn);
    //     }
    // }

    protected async queryPromised(query: string, cancelObj: any, options: any) {
        options = options || {};
        try {
            const conn = await this.pool.acquire();
            try {
                const database = conn.db(this.config.database);

                const aggregationQuery = [
                    {
                        $sql: {
                            statement: query,
                            format: "jdbc",
                            dialect: "mongosql"
                        }
                    }
                ];

                return database.aggregate(aggregationQuery)
            } finally {
                await this.pool.release(conn);
            }
        } catch (ex: any) {
            if (ex.cause) {
                throw new Error(ex.cause.getMessageSync());
            } else {
                throw ex;
            }
        }
    }

    public async streamQuery(sql: string, values: string[]): Promise<Readable> {
        // TODO: implement
        console.log('streamQuery not implemented');
    }

    public async release() {
        console.log('releasing pool ....');
        await this.pool.drain();
        await this.pool.clear();
    }

    public override async tableColumnTypes(table: string): Promise<TableStructure> {
        const [database, name] = table.split('.');
        const conn = await this.pool.acquire();

        try {
            const database = conn.db(this.config.database);
            const schema =


            const getMetaDataAsync = promisify(conn.getMetaData.bind(conn));

            const schema: TableStructure = await getMetaDataAsync().then(async (metadata: any) => {
                const getColumns = promisify(metadata.getColumns.bind(metadata));
                const columnsResults = await getColumns(null, database, name, null);

                const toObjArrayColumns = promisify(columnsResults.toObjArray.bind(columnsResults));
                const columnsResultsArray = await toObjArrayColumns();

                let columnTypes: TableStructure = [];
                columnsResultsArray.forEach((columnRow: any) => {
                    columnTypes.push({
                        name: columnRow.COLUMN_NAME,
                        type: this.toGenericType(columnRow.TYPE_NAME),
                    });
                });

                return columnTypes;
            });

            return schema;
        } catch (err) {
            console.error(err);
            throw err;
        } finally {
            await this.pool.release(conn);
        }
    }

    public override async tablesSchema(): Promise<SchemaStructure> {
        const conn = await this.pool.acquire();
        try {
            const getMetaDataAsync = promisify(conn.getMetaData.bind(conn));

            const schema: SchemaStructure = await getMetaDataAsync().then(async (metadata: any) => {

                const getTables = promisify(metadata.getTables.bind(metadata));
                const tableResults = await getTables(process.env.CUBEJS_DB_NAME, null, null, ['TABLE']);

                let schema: SchemaStructure = {};

                const toObjArray = promisify(tableResults.toObjArray.bind(tableResults));
                const tableResultsArray = await toObjArray();

                let tableSchema: any = {};

                for (const tableRow of tableResultsArray) {
                    const getColumns = promisify(metadata.getColumns.bind(metadata));
                    const columnsResults = await getColumns(null, tableRow.TABLE_SCHEM, tableRow.TABLE_NAME, null);

                    const toObjArrayColumns = promisify(columnsResults.toObjArray.bind(columnsResults));
                    const columnsResultsArray = await toObjArrayColumns();

                    columnsResultsArray.forEach((columnRow: any) => {
                        const tables = (tableSchema[columnRow.TABLE_NAME] || []);
                        tables.push({
                            name: columnRow.COLUMN_NAME,
                            type: columnRow.TYPE_NAME,
                            attributes: columnRow.KEY_TYPE ? ['primaryKey'] : []
                        });
                        tables.sort();
                        tableSchema[columnRow.TABLE_NAME] = tables;
                    });

                    schema[tableRow.TABLE_CAT] = tableSchema;
                }

                return schema;
            });

            return schema;

        } catch (err) {
            console.error(err);
            throw err;
        } finally {
            await this.pool.release(conn);
        }
    }

    public override async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {

        const resultSet: any= await this.queryRaw(query, values);

        const toObjArrayAsync = resultSet.toObjArray && promisify(resultSet.toObjArray.bind(resultSet));

        const rows = await toObjArrayAsync();

        const getMetaDataAsync = promisify(resultSet.getMetaData.bind(resultSet));
        const metaData = await getMetaDataAsync();

        const types: any = [];
        const getColumnCount = promisify(metaData.getColumnCount.bind(metaData));
        for (let i = 1; i <= await getColumnCount(); i++) {
            const getColumnName = promisify(metaData.getColumnName.bind(metaData));
            const getColumnTypeName = promisify(metaData.getColumnTypeName.bind(metaData));
            types.push({
                name: await getColumnName(i),
                type: this.toGenericType(await getColumnTypeName(i))
            })
        }

        return {
            rows,
            types,
        };
    }

    public override toGenericType(columnType: string) {
        if (columnType === 'null') {
            return 'text';
        }
        return super.toGenericType(columnType);
    }
}



