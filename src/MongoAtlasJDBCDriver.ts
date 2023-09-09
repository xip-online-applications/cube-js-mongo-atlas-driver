/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `MongoAtlasJDBC` and related types declaration.
 */

import {
    getEnv,
    assertDataSource,
} from '@cubejs-backend/shared';
import fs from 'fs';
import path from 'path';
import {
    JDBCDriver,
    JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import {
    DriverInterface,
    TableQueryResult,
    TableStructure,
    DownloadQueryResultsOptions,
    DownloadQueryResultsResult,
    Row
} from '@cubejs-backend/base-driver';
import {downloadJDBCDriver} from './installer';
import {MongoAtlasJDBCQuery} from './MongoAtlasJDBCQuery';
import {promisify} from "util";
import {SchemaStructure} from "@cubejs-backend/base-driver/dist/src/driver.interface";

export type MongoAtlasJDBCConfiguration = JDBCDriverConfiguration;

const ResultSetMetaData = require('@cubejs-backend/jdbc/lib/resultsetmetadata');

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
};


async function fileExistsOr(
    fsPath: string,
    fn: () => Promise<string>,
): Promise<string> {
    if (fs.existsSync(fsPath)) {
        return fsPath;
    }
    return fn();
}

async function resolveJDBCDriver(): Promise<string> {
    return fileExistsOr(
        path.join(process.cwd(), 'MongoAtlasJDBC.jar'),
        async () => fileExistsOr(
            path.join(__dirname, '..', 'download', 'MongoAtlasJDBC.jar'),
            async () => {
                const pathOrNull = await downloadJDBCDriver();
                if (pathOrNull) {
                    return pathOrNull;
                }
                throw new Error(
                    'Please download and place MongoAtlasJDBC.jar inside your ' +
                    'project directory'
                );
            }
        )
    );
}

/**
 * Mongo Atlas JDBC driver class.
 */
export class MongoAtlasJDBCDriver extends JDBCDriver implements DriverInterface {

    /**
     * Class constructor.
     */
    public constructor(
        conf: Partial<MongoAtlasJDBCConfiguration> & {
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
             * request before determining it as not valid. Default - 10000 ms.
             */
            testConnectionTimeout?: number,
        } = {},
    ) {
        const dataSource =
            conf.dataSource ||
            assertDataSource('default');

        const url: string =
            conf?.url ||
            getEnv('jdbcUrl', {dataSource});

        const config: MongoAtlasJDBCConfiguration = {
            ...conf,
            url,
            dbType: 'mongo-atlas-jdbc',
            drivername: 'com.mongodb.jdbc.MongoDriver',
            properties: {
                user: process.env.CUBEJS_DB_USER,
                password: process.env.CUBEJS_DB_PASS,
                port: process.env.CUBEJS_DB_PORT
            }
        } as MongoAtlasJDBCConfiguration;

        super(config);
    }

    public static dialectClass() {
        return MongoAtlasJDBCQuery;
    }

    public override readOnly() {
        return true;
    }

    protected override async getCustomClassPath() {
        return resolveJDBCDriver();
    }

    public async getTablesQuery(database: string): Promise<TableQueryResult[]> {
        const conn = await this.pool.acquire();
        try {
            const getMetaDataAsync = promisify(conn.getMetaData.bind(conn));

            const schema: TableQueryResult[] = await getMetaDataAsync().then(async (metadata: any) => {
                const getTables = promisify(metadata.getTables.bind(metadata));
                const tableResults = await getTables(null, database, null, ['TABLE']);

                const toObjArray = promisify(tableResults.toObjArray.bind(tableResults));
                const tableResultsArray = await toObjArray();


                let tableTypes: TableQueryResult[] = [];
                tableResultsArray.forEach((table: any) => {
                    tableTypes.push({
                        table_name: table.TABLE_NAME,
                        TABLE_NAME: table.TABLE_NAME
                    });
                });

                return tableTypes;
            });

            return schema;
        } catch (err) {
            console.error(err);
            throw err;
        } finally {
            await this.pool.release(conn);
        }
    }

    public override async tableColumnTypes(table: string): Promise<TableStructure> {
        const [database, name] = table.split('.');
        const conn = await this.pool.acquire();

        try {
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

    override async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
        const resultSet: any = await super.query(query, values);
        const toObjArrayAsync = resultSet.toObjArray && promisify(resultSet.toObjArray.bind(resultSet));

        const rows = await toObjArrayAsync();

        return rows;
    }

    protected override async executeStatement(conn: any, query: any, cancelObj?: any) {

        const createStatementAsync = promisify(conn.createStatement.bind(conn));
        const statement = await createStatementAsync();
        if (cancelObj) {
            cancelObj.cancel = promisify(statement.cancel.bind(statement));
        }
        const setQueryTimeout = promisify(statement.setQueryTimeout.bind(statement));
        await setQueryTimeout(600);
        const executeQueryAsync = promisify(statement.execute.bind(statement));
        const resultSet = await executeQueryAsync(query);

        return resultSet;
    }

    public override async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {

        const resultSet: any= await super.query(query, values);

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
