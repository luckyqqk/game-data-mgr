/**
 * 表结构信息
 * @param name
 * @constructor
 */
var Structure = function(name) {
    this.name = name;       // 表名
    this.primary = null;    // 主键名
    this.autoIncrement = 0; // 是否自增
    this.foreign = null;    // 外键名
    this.sons = null;       // 子表名
    this.fields = {};       // 表字段
    this.redisType = 'list';// redis缓存结构
};

module.exports = Structure;
// 最终能转成node中的number类型的字段类型,不包含bigint
const NUMBER_TYPES = ['int', 'integer', 'tinyint', 'smallint', 'mediumint'];

function _isNumber(type) {
    for (let i = 0; i < NUMBER_TYPES.length; i++) {
        if (type.startsWith(NUMBER_TYPES[i]))
            return true;
    }
    return false;
}

/**
 * 添加字段信息
 * @param columns
 */
Structure.prototype.addColumns = function(columns) {
    // [ { Field: 'id',
    //     Type: 'int(11) unsigned',
    //     Null: 'NO',
    //     Key: 'PRI',
    //     Default: null,
    //     Extra: 'auto_increment' },
    //     { Field: 'userId',
    //         Type: 'int(11) unsigned',
    //         Null: 'NO',
    //         Key: '',
    //         Default: null,
    //         Extra: '' },
    //     { Field: 'gold',
    //         Type: 'int(11) unsigned',
    //         Null: 'NO',
    //         Key: '',
    //         Default: '10000',
    //         Extra: '' },
    //     { Field: 'diamond',
    //         Type: 'int(11) unsigned',
    //         Null: 'NO',
    //         Key: '',
    //         Default: '0',
    //         Extra: '' } ]
    columns.forEach(col=>{
        this.fields[col["Field"]] = col['Default'] != null && _isNumber(col['Type']) ? parseInt(col['Default']) : col['Default'];
        if (col["Key"] == "PRI")
            this.primary = col["Field"];
        if (col["Extra"] == 'auto_increment')
            this.autoIncrement = 1;
    });
    // console.error(this.fields);
};

/**
 * 添加关联信息
 * @param referInfo
 */
Structure.prototype.addReference = function(referInfo) {
    this.foreign = referInfo['foreignKey'];
    this.sons = referInfo['sonTables'];
    this.redisType = referInfo['redisType'] || this.redisType;
};