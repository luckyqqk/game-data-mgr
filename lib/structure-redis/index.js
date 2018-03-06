var StructureRedis = function(redis) {
    this.redis = redis;
};

module.exports = StructureRedis;

/**
 * 将表结构信息存入redis
 * @param {string}      sign                数据库标识
 * @param {obj|Array}   structure           mysql table structure, support array
 * @param {string}      structure.name      tableName
 * @param cb
 */
StructureRedis.prototype.setStructure = function(sign, structure, cb) {
    var valueArr = [];
    var toRedisValue = function(tableInfo) {
        valueArr.push(tableInfo['name']);
        valueArr.push(JSON.stringify(tableInfo));
    };
    if (Array.isArray(structure)) {
        structure.forEach(tInfo=>{
            toRedisValue(tInfo);
        });
    } else {
        toRedisValue(structure);
    }
    this.redis.hmset(sign, valueArr, (err)=>{
        cb(err);
    });
};

/**
 * 根据表名获取表的表结构信息(推荐)
 * @param {string}          sign            数据库标识
 * @param {string|Array}    tableName       support array
 * @param cb
 */
StructureRedis.prototype.getStructure = function(sign, tableName, cb) {
    var redisArr = [];
    var pushToArr = function(name) {
        redisArr.push(['hget', sign, name]);
    };
    if (Array.isArray(tableName)) {
        tableName.forEach(name=>{
            pushToArr(name);
        });
    } else {
        pushToArr(tableName);
    }
    this.redis.multi(redisArr).exec((err, data)=>{
        if (!!err)
            cb(err);
        else if (!data)
            cb(null, null);
        else {
            if (!Array.isArray(data)) {
                cb(null, JSON.parse(data));
            } else if (data.length == 1) {
                cb(null, JSON.parse(data[0]));
            } else {
                var res = [];
                data.forEach(d=>{
                    res.push(JSON.parse(d));
                });
                cb(null, res);
            }
        }
    });
};

/**
 * 清除表结构信息
 * @param {string}          sign            数据库标识
 * @param cb
 */
StructureRedis.prototype.clearStructure = function(sign, cb) {
    this.redis.del(sign, cb);
};

/**
 * 获取所有表结构(不推荐)
 * @param {string}          sign            数据库标识
 * @param cb
 */
StructureRedis.prototype.getStructures = function(sign, cb) {
    this.redis.hgetall(sign, cb);
};

/**
 * 是否表结构信息已经存入redis
 * @param sign
 * @param cb
 */
StructureRedis.prototype.hasStructure = function(sign, cb) {
    this.redis.exists(sign, cb);
};

//StructureRedis.prototype.destroy = function() {
//    if (!!this.redis)
//        this.redis = null;
//};