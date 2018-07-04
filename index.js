var format = require('util').format;
var DBMgr = require('./lib/db-mgr/index');
var SQLMaker = require('./lib/sql-maker/index');
var GameRedis = require('./lib/game-redis/index');
var StructureMgr = require('./lib/structure/index');
var StructureRedis = require('./lib/structure-redis/index');

/**
 * 游戏数据管理器
 * @param {object}  mysqlPool       数据库连接池
 * @param {object}  redisClient     redis客户端
 * @param {object}  tableReference  表关联关系
 * @param {string}  databaseSign    数据库独立标识
 * @param {string}  database        数据库名
 * @constructor
 */
var DataMgr = function (mysqlPool, redisClient, tableReference, databaseSign, database) {
    this.pool = mysqlPool;
    this.redis = redisClient;
    this.databaseSign = databaseSign;
    this.tableReference = tableReference;

    this.dbMgr = new DBMgr(this.pool);
    this.gameRedis = new GameRedis(this.redis);
    this.structureRedis = new StructureRedis(this.redis);
    this.structureMgr = new StructureMgr(database);
};

module.exports = DataMgr;
var PRIMARY = 'primary';
var FOREIGN = 'foreign';

/**
 * 加载表结构信息
 * @param cb
 */
DataMgr.prototype.loadTableStructure = function (cb) {
    var self = this;
    self.structureRedis.hasStructure(self.databaseSign, (err, has)=> {
        if (!!has) {    // 表结构信息加载一次就够了,如果表结构被更改,请手动删除redis中的表结构数据.
            cb();
            return;
        }
        self.structureMgr.readStructure(self.pool, (err, data)=> {
            if (!!err) {
                cb(err);
                return;
            }
            self.structureMgr.addReference(self.tableReference);
            self.structureRedis.setStructure(self.databaseSign, data, err=> {
                cb(err);
            });
        });
    });
};

DataMgr.prototype.destroy = function () {
    if (!!this.pool)
        this.pool.end();
    this.redis = null;
    this.dbMgr = null;
    this.structureRedis = null;
    this.gameRedis = null;
};

/**
 * 插入数据
 * @param {string}          tableName
 * @param {object|Array}    jsonArray
 * @param {bool}            [noCache]   true->不操作缓存,可不传
 * @param cb
 */
DataMgr.prototype.insertData = function (tableName, jsonArray, noCache, cb) {
    if (typeof noCache == 'function') {
        cb = noCache;
        noCache = false;
    }
    if (!tableName || !jsonArray || jsonArray.length < 1) {
        cb(`params null`);
        return;
    }
    var self = this;
    self.structureRedis.getStructure(self.databaseSign, tableName, (err, structure)=> {
        if (!!err || !structure) {
            cb(err || `${tableName} can not find structure`);
            return;
        }
        var makeArr = SQLMaker.makeInsertSql(structure, jsonArray);
        var sql = makeArr[0], newJsonArray = makeArr[1];
        self.dbMgr.query(sql, [], function (err, data) {
            if (!!err) {
                cb(err);
                console.error(`sql:${sql}`);
                return;
            }
            var insertId = data.insertId;
            if (Array.isArray(newJsonArray)) {
                newJsonArray.forEach(json=> {
                    json[structure[PRIMARY]] = insertId++;
                });
            } else
                newJsonArray[structure[PRIMARY]] = insertId;
            if (!noCache)
                self.gameRedis.addRedisCache(structure, newJsonArray, (err)=> {
                    cb(err, newJsonArray);
                });
            else
                cb(null, newJsonArray);
        });
    });
};

/**
 * 删除数据
 * @param {string}  tableName
 * @param priValue
 * @param forValue
 * @param {bool}    [noCache]   true->不操作缓存,可不传
 * @param cb
 */
DataMgr.prototype.deleteData = function (tableName, priValue, forValue, noCache, cb) {
    if (typeof noCache == 'function') {
        cb = noCache;
        noCache = false;
    }
    if (!priValue && !forValue) {
        cb('mast has primary value or foreign value');
        return;
    }

    var self = this;
    self.structureRedis.getStructure(self.databaseSign, tableName, (err, structure)=> {
        if (!!err || !structure) {
            cb(err || `${tableName} can not find structure`);
            return;
        }
        var condition = !!priValue ? format('`%s` = ', structure[PRIMARY], priValue) : format('`%s` = ', structure[FOREIGN], forValue);
        var delSql = SQLMaker.makeDeleteSql(tableName, condition);
        self.dbMgr.query(delSql, [], (err)=> {
            if (!!err) {
                cb(err);
                console.error(delSql);
            } else {
                noCache ? cb() : self.gameRedis.removeRedisCache(structure, priValue, forValue, cb);
            }
        });
    });
};

/**
 * 更新数据
 * @param {string}  tableName
 * @param {object}  jsonValue
 * @param {bool}    [noCache]   true->不操作缓存,可不传
 * @param cb
 */
DataMgr.prototype.updateData = function (tableName, jsonValue, noCache, cb) {
    if (typeof noCache == 'function') {
        cb = noCache;
        noCache = false;
    }
    var self = this;
    self.structureRedis.getStructure(self.databaseSign, tableName, (err, structure)=> {
        if (!!err || !structure) {
            cb(err || `${tableName} can not find structure`);
            return;
        }
        var makeArr = SQLMaker.makeUpdateSql(structure, jsonValue);
        if (makeArr == null) {
            cb(`${tableName} update failed, can't find primary value`);
            return;
        }
        var sql = makeArr[0], newObj = makeArr[1];
        self.dbMgr.query(sql, [], err=> {
            if (!!err) {
                cb(err);
                console.error(sql);
            } else {
                noCache ? cb(null, newObj) : self.gameRedis.updateRedisCache(structure, newObj, cb);
            }
        });
    });
};

/**
 * 查找数据
 * @param {string}  tableName
 * @param priValue
 * @param forValue
 * @param {bool}    [noCache]   true->不操作缓存,可不传
 * @param cb
 */
DataMgr.prototype.selectData = function (tableName, priValue, forValue, noCache, cb) {
    if (typeof noCache == 'function') {
        cb = noCache;
        noCache = false;
    }
    if (!tableName || !priValue && !forValue) {
        cb("selectData failed :: param is null");
        return;
    }
    var self = this;
    self.structureRedis.getStructure(self.databaseSign, tableName, (err, structure)=> {
        if (!!err || !structure) {
            cb(err || `${tableName} can not find structure`);
            return;
        }
        _selectData(self, structure, priValue, forValue, noCache, cb);
    });
};

/**
 * 根据条件取数据
 * @param {string}  tableName
 * @param {object}  condition   返回符合条件的数据,必须含有主键值或者外键值
 * @param {bool}    [noCache]   true->不操作缓存,可不传
 * @param cb
 */
DataMgr.prototype.selectDataByCondition = function (tableName, condition, noCache, cb) {
    if (typeof noCache == 'function') {
        cb = noCache;
        noCache = false;
    }
    var self = this;
    self.structureRedis.getStructure(self.databaseSign, tableName, (err, structure)=> {
        if (!!err || !structure) {
            cb(err || `${tableName} can not find structure`);
            return;
        }
        var priValue = condition[structure[PRIMARY]],
            forValue = condition[structure[FOREIGN]];
        _selectData(self, structure, priValue, forValue, noCache, (err, data)=> {
            if (!!err || !data) {
                cb(err);
                return;
            }
            var isInCondition = function (json) {
                for (let key in condition) {
                    if (condition.hasOwnProperty(key) && condition[key] != json[key])
                        return false;
                }
                return true;
            };
            var result = [];
            data.forEach((aData)=> {
                if (isInCondition(aData))
                    result.push(aData);
            });
            cb(null, result);
        });
    });
};

/**
 * 查看数据是否存在
 * @param {string}  tableName
 * @param {object}  condition
 * @param cb
 */
DataMgr.prototype.isExist = function (tableName, condition, cb) {
    if (!tableName || !condition) {
        cb('tableName or condition can not be null');
        return;
    }
    var cond = '';
    for (let k in condition) {
        if (!condition.hasOwnProperty(k))
            continue;
        var valueFix = isNaN(condition[k]) ? format('"%s"', condition[k]) : condition[k];
        cond += format("`%s` = %s and", k, valueFix);
    }
    cond = cond.substr(0, cond.length - 4);
    var sql = format("select * from `%s` where %s", tableName, cond);
    this.dbMgr.query(sql, [], function (err, dataDB) {
        !!err || !dataDB || dataDB.length < 1 ? cb(err, false) : cb(null, true);
    });
};

/**
 * 清除缓存,不删除数据库.
 * 根据根表和根表主键值,删除其和其下相关的数据缓存.
 * @param tableName         父级表名
 * @param primaryValue      必填
 * @param [foreignValue]    没外键不用添
 * @param cb
 */
DataMgr.prototype.deleteRedisCacheByFather = function (tableName, primaryValue, foreignValue, cb) {
    var self = this;
    self.structureRedis.getStructures(self.databaseSign, (err, structure)=> {
        if (!!err || !structure) {
            cb(err || `${tableName} can not find structure`);
            return;
        }
        self.gameRedis.removeCacheByFather(tableName, structure, primaryValue, foreignValue, cb);
    });
};


/**
 * 查找数据的私有方法
 * @private
 */
function _selectData(self, structure, priValue, forValue, noCache, cb) {
    !noCache ?
        _selectFromRedis(self, structure, priValue, forValue, cb) :
        _selectFromDB(self, structure, priValue, forValue, cb);
}

/**
 * 从数据库查找数据
 * @private
 */
function _selectFromDB(self, structure, priValue, forValue, cb) {
    var condition = null;
    if (!!priValue)
        condition = format('`%s` = ', structure[PRIMARY], priValue);
    else if (!!forValue) {
        forValue = isNaN(forValue) ? format('"%s"', forValue) : forValue;
        condition = format('`%s` = ', structure[FOREIGN], forValue);
    } else {
        cb(`select need primary value or foreign value`);
        return;
    }
    var sql = SQLMaker.makeSelectSql(structure.name, condition);
    self.dbMgr.query(sql, [], function (err, dataDB) {
        if (!!err || !dataDB || dataDB.length < 1) {
            cb(err, !!priValue ? null : []);
            if (!!err) console.error(`sql:${sql}`);
        } else {
            var data = JSON.parse(JSON.stringify(dataDB));
            cb(null, !!priValue ? data[0] : data);
        }
    });
}

/**
 * 优先缓存查找,其次从数据库查找
 * @private
 */
function _selectFromRedis(self, structure, priValue, forValue, cb) {
    self.gameRedis.getRedisCache(structure, priValue, forValue, (err, data)=> {
        if (!!err || !!data) {
            cb(err, data);
        } else {
            _selectFromDB(self, structure, priValue, forValue, (err, dbData)=> {
                if (!!err || !dbData || dbData.length < 1) {
                    cb(err, dbData);
                } else {
                    self.gameRedis.addRedisCache(structure, dbData, err=> {
                        cb(err, dbData);
                    });
                }
            });
        }
    });
}