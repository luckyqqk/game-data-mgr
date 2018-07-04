var async = require('async');
var format = require('util').format;
var Structure = require('./structure');

var StructureMgr = function(database) {
    this.database = database;
    this.tables = null;
};

module.exports = StructureMgr;

/**
 * 获取数据库标识
 * @returns {string}
 */
StructureMgr.prototype.getSign = function() {
    return this.sign;
};

/**
 * 获取表结构信息
 * @returns {object}
 */
StructureMgr.prototype.getStructure = function() {
    return this.tables;
};
// desc table return
//{
//    Field: 'ID',
//    Type: 'int(11)',
//    Null: 'NO',
//    Key: 'PRI',
//    Default: null,
//    Extra: 'auto_increment'
// }
/**
 * 仅仅收录表结构
 * @param connection
 * @param cb
 */
StructureMgr.prototype.readStructure = function(connection, cb) {
    var self = this;
    connection.query("show tables;", [], function (err, data) {
        if (!!err) {
            cb(err);
            return;
        } else if (!data || data.length < 1) {
            cb(`no tables in database:${self.database}`);
            return;
        }
        // 表结构信息
        var _describeTable = function(tableName) {
            return function(cb) {
                connection.query(`describe ${tableName}`, [], (err, columns)=> {
                    if (!!err) {
                        cb(err);
                        return;
                    }
                    // console.error(columns);
                    columns = JSON.parse(JSON.stringify(columns));
                    var table = new Structure(tableName);
                    // console.error(columns);
                    table.addColumns(columns);
                    cb(null, table);
                });
            };
        };
        data = JSON.parse(JSON.stringify(data));
        var tableKey = `Tables_in_${self.database}`;
        var funcArray = [];
        data.forEach(t=>{
            funcArray.push(new _describeTable(t[tableKey]));
        });
        async.parallel(funcArray, (err, tablesInfo)=>{
            cb(null, self.tables = tablesInfo);
        });
    });
};

/**
 * 增加关联信息
 * @param {object}  referObj    当前用excel表配置的表关联关系,也可以读取MySQL外键配置(不推荐,因为InnoDB才支持)
 */
StructureMgr.prototype.addReference = function(referObj) {
    if (!this.tables || !referObj)
        return;
    var referInfo = null;
    this.tables.forEach(table=>{
        referInfo = referObj[table.name];
        if (!referInfo) {
            console.warn(`structure load : table ${table.name} has no reference`);
            return;
        }
        table.addReference(referInfo);
    });
};