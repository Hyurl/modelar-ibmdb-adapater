import { Adapter, DB, Table, Query, DBConfig } from "modelar";
import { Pool, Database } from "ibm_db";

function getConnectionString(config: DBConfig): string {
    let pairs = [],
        map = {
            database: "DATABASE",
            protocol: "PROTOCOL",
            host: "HOSTNAME",
            port: "PORT",
            user: "UID",
            password: "PWD",
        };

    for (let key in map) {
        if (config[key])
            pairs.push(map[key] + "=" + config[key]);
    }

    return pairs.join(";");
}

export class IbmdbAdapter extends Adapter {
    connection: Database;
    backquote = "\"";

    static readonly Pools: { [dsn: string]: Pool } = {};

    connect(db: DB): Promise<DB> {
        let constr = db.config["connectionString"] || getConnectionString(db.config);

        if (IbmdbAdapter.Pools[db.dsn] === undefined) {
            IbmdbAdapter.Pools[db.dsn] = new Pool();
            IbmdbAdapter.Pools[db.dsn].setMaxPoolSize(db.config.max);
            IbmdbAdapter.Pools[db.dsn].setConnectTimeout(db.config.timeout);
        }

        return new Promise((resolve, reject) => {
            IbmdbAdapter.Pools[db.dsn].open(constr, (err, connection) => {
                if (err) {
                    reject(err);
                } else {
                    this.connection = connection;
                    resolve(db);
                }
            });
        });
    }

    query(db: DB, sql: string, bindings?: any[]): Promise<DB> {
        var affectCommands = ["insert", "update", "delete"],
            affected = false;

        return new Promise((resolve, reject) => {
            if (affectCommands.indexOf(db.command) >= 0) {
                let middle = db.command == "delete" ? "old" : "new";
                sql = `select count(*) as COUNT from ${middle} table (${sql})`;
                affected = true;
            }

            this.connection.query(sql, bindings, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    if (affected) {
                        db.affectedRows = rows[0].COUNT;
                    } else {
                        db.data = rows;
                        db.affectedRows = rows.length;

                        for (let row of rows) {
                            delete row["_rn"];
                        }
                    }

                    if (db.command === "insert") {
                        this.connection.query(
                            "select identity_val_local() from SYSIBM.SYSDUMMY1",
                            (err, rows) => {
                                if (err) {
                                    reject(err);
                                } else {
                                    db.insertId = parseInt(rows[0][1]);
                                    resolve(db);
                                }
                            }
                        );
                    } else {
                        resolve(db);
                    }
                }
            });
        });
    }

    transaction(db: DB, cb: (db: DB) => void): Promise<DB> {
        var promise = new Promise((resolve: (db: DB) => void, reject) => {
            this.connection.beginTransaction(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve(db)
                }
            });
        });

        if (typeof cb == "function") {
            return promise.then(db => {
                var res = cb.call(db, db);

                if (res.then instanceof Function) {
                    return res.then(() => db) as Promise<DB>;
                } else {
                    return db;
                }
            }).then(db => {
                return this.commit(db);
            }).catch(err => {
                return this.rollback(db).then(db => {
                    throw err;
                });
            });
        } else {
            return promise;
        }
    }

    commit(db: DB): Promise<DB> {
        return new Promise((resolve, reject) => {
            this.connection.commitTransaction(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve(db);
                }
            });
        });
    }

    rollback(db: DB): Promise<DB> {
        return new Promise((resolve, reject) => {
            this.connection.rollbackTransaction(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve(db);
                }
            });
        });
    }

    release(): void {
        this.close();
        // this.connection = null;
    }

    close(): void {
        if (this.connection) {
            this.connection.close();
            this.connection = null;
        }
    }

    static close(): void {
        for (let i in IbmdbAdapter.Pools) {
            IbmdbAdapter.Pools[i].close(() => null);
            delete IbmdbAdapter.Pools[i];
        }
    }

    getDDL(table: Table) {
        let numbers = ["int", "integer"],
            columns: string[] = [],
            foreigns: string[] = [];
        let primary: string;
        let autoIncrement: string;

        for (let key in table.schema) {
            let field = table.schema[key];

            if (field.primary && field.autoIncrement) {
                if (numbers.indexOf(field.type.toLowerCase()) === -1) {
                    field.type = "int";
                }

                autoIncrement = " generated always as identity (start with "
                    + field.autoIncrement[0]
                    + ", increment by "
                    + field.autoIncrement[1]
                    + ")";
            } else {
                autoIncrement = null;
            }

            let type = field.type;
            if (field.length instanceof Array) {
                type += "(" + field.length.join(",") + ")";
            } else if (field.length) {
                type += "(" + field.length + ")";
            }

            let column = table.backquote(field.name) + " " + type;

            if (field.primary)
                primary = field.name;

            if (field.unique)
                column += " unique";

            if (field.unsigned)
                column += " unsigned";

            if (field.notNull)
                column += " not null";

            if (field.default === null)
                column += " default null";
            else if (field.default !== undefined)
                column += " default " + table.quote(field.default);

            if (field.comment)
                column += " comment " + table.quote(field.comment);

            if (autoIncrement)
                column += autoIncrement;

            if (field.foreignKey && field.foreignKey.table) {
                let foreign = "constraint " + table.backquote(field.name + "_frk")
                    + ` foreign key (${table.backquote(field.name)})`
                    + " references " + table.backquote(field.foreignKey.table)
                    + " (" + table.backquote(field.foreignKey.field) + ")"
                    + " on delete " + field.foreignKey.onDelete
                    + " on update " + field.foreignKey.onUpdate;

                foreigns.push(foreign);
            };

            columns.push(column);
        }

        let sql = "create table " + table.backquote(table.name) +
            " (\n  " + columns.join(",\n  ");

        if (primary)
            sql += ",\n  primary key (" + table.backquote(primary) + ")";

        if (foreigns.length)
            sql += ",\n  " + foreigns.join(",\n  ");

        return sql + "\n)";
    }

    /** Methods for Query */

    limit(query: Query, length: number, offset?: number): Query {
        if (!offset) {
            query["_limit"] = length;
        } else {
            query["_limit"] = [offset, length];
        }
        return query;
    }

    getSelectSQL(query: Query): string {
        let selects: string = query["_selects"];
        let distinct: string = query["_distinct"];
        let join: string = query["_join"];
        let where: string = query["_where"];
        let orderBy: string = query["_orderBy"];
        let groupBy: string = query["_groupBy"];
        let having: string = query["_having"];
        let union: string = query["_union"];
        let limit: number | [number, number] = <any>query["_limit"];
        let isCount = (/count\(distinct\s\S+\)/i).test(selects);
        let paginated = limit instanceof Array;

        distinct = distinct && !isCount ? "distinct " : "";
        where = where ? ` where ${where}` : "";
        orderBy = orderBy ? `order by ${orderBy}` : "";
        groupBy = groupBy ? ` group by ${groupBy}` : "";
        having = having ? ` having ${having}` : "";
        union = union ? ` union ${union}` : "";

        let sql = "select " + distinct + selects;

        if (paginated)
            sql += `, row_number() over(${orderBy}) "_rn"`;

        sql += " from " +
            (!join ? query.backquote(query.table) : "") + join + where;

        sql += groupBy + having + union;

        if (!paginated && orderBy)
            sql += ` ${orderBy}`;

        if (limit) {
            if (paginated) {
                sql = `select * from (${sql}) tmp where tmp."_rn" > ${limit[0]}`
                    + ` and tmp."_rn" <= ${limit[0] + limit[1]}`;
            } else {
                sql += ` fetch first ${limit} rows only`;
            }
        }

        return sql;
    }
}

export default IbmdbAdapter;
