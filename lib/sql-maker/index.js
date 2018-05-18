var format = require('util').format;
var SQLMaker = module.exports;
/**
 * 构建插入语句
 * @param {object} structure
 * @param {string} structure.name           table name
 * @param {string} structure.primary        primary key
 * @param {object} structure.autoIncrement  是否主键自增
 * @param {object} structure.fields         table fields
 * @param {object|Array}    jsonArray
 * @returns {Array}         Array
 * @returns {string}        Array[0] sql
 * @returns {object|Array}  Array[1] newObject
 */
SQLMaker.makeInsertSql = function(structure, jsonArray) {
    var fields = structure.fields,
        keys = '';
    // make key
    for (let field in fields) {
        if (!fields.hasOwnProperty(field) || typeof field != 'string')
            continue;
        if (structure.autoIncrement && structure.primary == field)
            continue;
        keys += format('`%s`,', field);
    }
    keys = keys.substring(0, keys.length - 1);
    // make one data value
    var _makeValue = function(json) {
        var newObj = {},
            values = '';
        for (let field in fields) {
            if (!fields.hasOwnProperty(field) || typeof field != 'string')
                continue;
            var value = json[field] || fields[field];
            newObj[field] = value;
            if (structure.autoIncrement && structure.primary == field)
                continue;
            value = isNaN(value) ? format('"%s"', value) : value;
            values += format('%s,', value);
        }
        values = values.substring(0, values.length - 1);
        return [values, newObj];
    };
    var values = '',
        newObj;
    // make values
    if (Array.isArray(jsonArray)) {
        newObj = [];
        jsonArray.forEach(json=>{
            var makeArr = _makeValue(json);
            values += format('(%s)', makeArr[0]);
            newObj.push(makeArr[1]);
        });
    } else {
        var makeArr = _makeValue(jsonArray);
        values = format('(%s)', makeArr[0]);
        newObj = makeArr[1];
    }
    var sql = format('insert into `%s` (%s) values %s', structure.name, keys, values);
    return [sql, newObj];
};

/**
 * 构建删除语句
 * @param {string}  tableName
 * @param {string}  condition
 * @returns {string}
 */
SQLMaker.makeDeleteSql = function(tableName, condition) {
    if (!condition || typeof condition != 'string')
        return null;
    else
        return format('delete from `%s` where %s', tableName, condition);
};

/**
 * 构建更新语句
 * @param {object} structure
 * @param {string} structure.name           table name
 * @param {string} structure.primary        primary key
 * @param {object} structure.autoIncrement  是否主键自增
 * @param {object} structure.fields         table fields
 * @param {object} json
 * @returns {Array}         Array
 * @returns {string}        Array[0] sql
 * @returns {object|Array}  Array[1] newObject
 */
SQLMaker.makeUpdateSql = function(structure, json) {
    var priValue = json[structure.primary];
    if (!priValue)
        return null;
    var afterObj = {},
        values = '',
        fields = structure.fields;
    for (let field in fields) {
        if (!fields.hasOwnProperty(field) || typeof field != 'string')
            continue;
        var value = json[field] || fields[field];
        afterObj[field] = value;
        if (structure.autoIncrement && structure.primary == field)
            continue;
        value = typeof value == 'string' || isNaN(value) ? format('"%s"', value) : value;
        values += format(' `%s` = %s,', field, value);
    }
    values = values.substring(0, values.length - 1);
    var sql = format('update `%s` set %s where `%s` = ', structure.name, values, structure.primary, priValue);
    return [sql, afterObj];
};

/**
 * 构建查询语句
 * @param {string}  tableName
 * @param {string}  condition
 * @returns {string}
 */
SQLMaker.makeSelectSql = function(tableName, condition) {
    if (!condition || typeof condition != 'string')
        return null;
    else
        return format('select * from `%s` where %s', tableName, condition);
};
