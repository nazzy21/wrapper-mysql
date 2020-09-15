import {Connect} from "./connect";
import {Collection} from "./collection";

export const name = 'MySQL';

/**
 Verify database configuration.

 @param {object} config
 	The configration data use to transact into the database.
 	For more details as to what the options should be, refer to https://github.com/mysqljs/mysql
**/
export async function assert(config) {
    if (!config.database) {
        return [new Error('No database name!')];
    }

    const conn = new Connect(config);

    return conn.verify();
}

export {Collection};