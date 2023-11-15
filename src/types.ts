import { Options } from 'generic-pool';

export type DriverConfiguration = {
    database: string,
    dbType: string,
    url: string,
    drivername: string,
    customClassPath?: string,
    properties: Record<string, any>,
    poolOptions?: Options;
    prepareConnectionQueries?: string[];
};
