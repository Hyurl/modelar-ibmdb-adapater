import { Adapter, DB, Table, Query } from "modelar";
import { Pool, Database } from "ibm_db";

export class IbmdbAdapter extends Adapter {
    connection: Database;
    backquote = "\"";

    static readonly Pools: { [dsn: string]: Pool } = {};

    connect(db: DB): Promise<DB> {
        let { database, host, port, user, password, max } = db.config;
        let constr = `DATABASE=${database};HOSTNAME=${host};PORT=${port};PROTOCOL=TCPIP;UID=${user};PWD=${password}`;

        if (IbmdbAdapter.Pools[db.dsn] === undefined) {
            IbmdbAdapter.Pools[db.dsn] = new Pool();
            IbmdbAdapter.Pools[db.dsn].setMaxPoolSize(max);
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
            if (affectCommands.includes(db.command)) {
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
                if (!numbers.includes(field.type.toLowerCase())) {
                    field.type = "int";
                }

                autoIncrement = ` generated always as identity (start with ${field.autoIncrement[0]}, increment by ${field.autoIncrement[1]})`;
            } else {
                autoIncrement = null;
            }

            if (field.length instanceof Array) {
                field.type += "(" + field.length.join(",") + ")";
            } else if (field.length) {
                field.type += "(" + field.length + ")";
            }

            let column = table.backquote(field.name) + " " + field.type;

            if (field.primary)
                primary = field.name;

            if (field.default === null)
                column += " default null";
            else if (field.default !== undefined)
                column += " default " + table.quote(field.default);

            if (field.notNull)
                column += " not null";

            if (field.unsigned)
                column += " unsigned";

            if (field.unique)
                column += " unique";

            if (field.comment)
                column += " comment " + table.quote(field.comment);

            if (autoIncrement)
                column += autoIncrement;

            if (field.foreignKey.table) {
                let foreign = `foreign key (${table.backquote(field.name)})` +
                    " references " + table.backquote(field.foreignKey.table) +
                    " (" + table.backquote(field.foreignKey.field) + ")" +
                    " on delete " + field.foreignKey.onDelete +
                    " on update " + field.foreignKey.onUpdate;

                foreigns.push(foreign);
            };

            columns.push(column);
        }

        let sql = "create table " + table.backquote(table.name) +
            " (\n\t" + columns.join(",\n\t");

        if (primary)
            sql += ",\n\tprimary key(" + table.backquote(primary) + ")";

        if (foreigns.length)
            sql += ",\n\t" + foreigns.join(",\n\t");

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
            sql += `, row_number() over(${orderBy}) rn`;

        sql += " from " +
            (!join ? query.backquote(query.table) : "") + join + where;

        if (!paginated && orderBy)
            sql += ` ${orderBy}`;

        sql += groupBy + having;

        if (limit) {
            if (paginated) {
                sql = `select * from (${sql}) tmp where tmp.rn > ${limit[0]} and tmp.rn <= ${limit[0] + limit[1]}`;
            } else {
                sql += ` fetch first ${limit} rows only`;
            }
        }

        return sql += union;
    }
}