"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var tslib_1 = require("tslib");
var modelar_1 = require("modelar");
var ibm_db_1 = require("ibm_db");
function getConnectionString(config) {
    var pairs = [], map = {
        database: "DATABASE",
        protocol: "PROTOCOL",
        host: "HOSTNAME",
        port: "PORT",
        user: "UID",
        password: "PASSWORD",
    };
    for (var key in map) {
        if (config[key])
            pairs.push(map[key] + "=" + config[key]);
    }
    return pairs.join(";");
}
var IbmdbAdapter = (function (_super) {
    tslib_1.__extends(IbmdbAdapter, _super);
    function IbmdbAdapter() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this.backquote = "\"";
        return _this;
    }
    IbmdbAdapter.prototype.connect = function (db) {
        var _this = this;
        var constr = db.config["connectionString"] || getConnectionString(db.config);
        if (IbmdbAdapter.Pools[db.dsn] === undefined) {
            IbmdbAdapter.Pools[db.dsn] = new ibm_db_1.Pool();
            IbmdbAdapter.Pools[db.dsn].setMaxPoolSize(db.config.max);
            IbmdbAdapter.Pools[db.dsn].setConnectTimeout(db.config.timeout);
        }
        return new Promise(function (resolve, reject) {
            IbmdbAdapter.Pools[db.dsn].open(constr, function (err, connection) {
                if (err) {
                    reject(err);
                }
                else {
                    _this.connection = connection;
                    resolve(db);
                }
            });
        });
    };
    IbmdbAdapter.prototype.query = function (db, sql, bindings) {
        var _this = this;
        var affectCommands = ["insert", "update", "delete"], affected = false;
        return new Promise(function (resolve, reject) {
            if (affectCommands.indexOf(db.command) >= 0) {
                var middle = db.command == "delete" ? "old" : "new";
                sql = "select count(*) as COUNT from " + middle + " table (" + sql + ")";
                affected = true;
            }
            _this.connection.query(sql, bindings, function (err, rows) {
                if (err) {
                    reject(err);
                }
                else {
                    if (affected) {
                        db.affectedRows = rows[0].COUNT;
                    }
                    else {
                        db.data = rows;
                        db.affectedRows = rows.length;
                        for (var _i = 0, rows_1 = rows; _i < rows_1.length; _i++) {
                            var row = rows_1[_i];
                            delete row["_rn"];
                        }
                    }
                    if (db.command === "insert") {
                        _this.connection.query("select identity_val_local() from SYSIBM.SYSDUMMY1", function (err, rows) {
                            if (err) {
                                reject(err);
                            }
                            else {
                                db.insertId = parseInt(rows[0][1]);
                                resolve(db);
                            }
                        });
                    }
                    else {
                        resolve(db);
                    }
                }
            });
        });
    };
    IbmdbAdapter.prototype.transaction = function (db, cb) {
        var _this = this;
        var promise = new Promise(function (resolve, reject) {
            _this.connection.beginTransaction(function (err) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(db);
                }
            });
        });
        if (typeof cb == "function") {
            return promise.then(function (db) {
                var res = cb.call(db, db);
                if (res.then instanceof Function) {
                    return res.then(function () { return db; });
                }
                else {
                    return db;
                }
            }).then(function (db) {
                return _this.commit(db);
            }).catch(function (err) {
                return _this.rollback(db).then(function (db) {
                    throw err;
                });
            });
        }
        else {
            return promise;
        }
    };
    IbmdbAdapter.prototype.commit = function (db) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this.connection.commitTransaction(function (err) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(db);
                }
            });
        });
    };
    IbmdbAdapter.prototype.rollback = function (db) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this.connection.rollbackTransaction(function (err) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(db);
                }
            });
        });
    };
    IbmdbAdapter.prototype.release = function () {
        this.close();
    };
    IbmdbAdapter.prototype.close = function () {
        if (this.connection) {
            this.connection.close();
            this.connection = null;
        }
    };
    IbmdbAdapter.close = function () {
        for (var i in IbmdbAdapter.Pools) {
            IbmdbAdapter.Pools[i].close(function () { return null; });
            delete IbmdbAdapter.Pools[i];
        }
    };
    IbmdbAdapter.prototype.getDDL = function (table) {
        var numbers = ["int", "integer"], columns = [], foreigns = [];
        var primary;
        var autoIncrement;
        for (var key in table.schema) {
            var field = table.schema[key];
            if (field.primary && field.autoIncrement) {
                if (numbers.indexOf(field.type.toLowerCase()) === -1) {
                    field.type = "int";
                }
                autoIncrement = " generated always as identity (start with "
                    + field.autoIncrement[0]
                    + ", increment by "
                    + field.autoIncrement[1]
                    + ")";
            }
            else {
                autoIncrement = null;
            }
            var type = field.type;
            if (field.length instanceof Array) {
                type += "(" + field.length.join(",") + ")";
            }
            else if (field.length) {
                type += "(" + field.length + ")";
            }
            var column = table.backquote(field.name) + " " + type;
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
                var foreign = "constraint " + table.backquote(field.name + "_frk")
                    + (" foreign key (" + table.backquote(field.name) + ")")
                    + " references " + table.backquote(field.foreignKey.table)
                    + " (" + table.backquote(field.foreignKey.field) + ")"
                    + " on delete " + field.foreignKey.onDelete
                    + " on update " + field.foreignKey.onUpdate;
                foreigns.push(foreign);
            }
            ;
            columns.push(column);
        }
        var sql = "create table " + table.backquote(table.name) +
            " (\n  " + columns.join(",\n  ");
        if (primary)
            sql += ",\n  primary key (" + table.backquote(primary) + ")";
        if (foreigns.length)
            sql += ",\n  " + foreigns.join(",\n  ");
        return sql + "\n)";
    };
    IbmdbAdapter.prototype.limit = function (query, length, offset) {
        if (!offset) {
            query["_limit"] = length;
        }
        else {
            query["_limit"] = [offset, length];
        }
        return query;
    };
    IbmdbAdapter.prototype.getSelectSQL = function (query) {
        var selects = query["_selects"];
        var distinct = query["_distinct"];
        var join = query["_join"];
        var where = query["_where"];
        var orderBy = query["_orderBy"];
        var groupBy = query["_groupBy"];
        var having = query["_having"];
        var union = query["_union"];
        var limit = query["_limit"];
        var isCount = (/count\(distinct\s\S+\)/i).test(selects);
        var paginated = limit instanceof Array;
        distinct = distinct && !isCount ? "distinct " : "";
        where = where ? " where " + where : "";
        orderBy = orderBy ? "order by " + orderBy : "";
        groupBy = groupBy ? " group by " + groupBy : "";
        having = having ? " having " + having : "";
        union = union ? " union " + union : "";
        var sql = "select " + distinct + selects;
        if (paginated)
            sql += ", row_number() over(" + orderBy + ") \"_rn\"";
        sql += " from " +
            (!join ? query.backquote(query.table) : "") + join + where;
        if (!paginated && orderBy)
            sql += " " + orderBy;
        sql += groupBy + having;
        if (limit) {
            if (paginated) {
                sql = "select * from (" + sql + ") tmp where tmp.\"_rn\" > " + limit[0]
                    + (" and tmp.\"_rn\" <= " + (limit[0] + limit[1]));
            }
            else {
                sql += " fetch first " + limit + " rows only";
            }
        }
        return sql += union;
    };
    IbmdbAdapter.Pools = {};
    return IbmdbAdapter;
}(modelar_1.Adapter));
exports.IbmdbAdapter = IbmdbAdapter;
exports.default = IbmdbAdapter;
//# sourceMappingURL=index.js.map