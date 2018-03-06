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

/**
 * 添加字段信息
 * @param columns
 */
Structure.prototype.addColumns = function(columns) {
    columns.forEach(col=>{
        this.fields[col["Field"]] = col['Default'];
        if (col["Key"] == "PRI")
            this.primary = col["Field"];
        if (col["Extra"] == 'auto_increment')
            this.autoIncrement = 1;
    });
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