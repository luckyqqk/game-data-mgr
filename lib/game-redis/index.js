var format = require('util').format;
/**
 * mysql数据表的redis缓存工具类
 * 封装好的CRUD方法,很适合手游研发的表数据缓存.
 * 缓存策略是根据父表id作为key,子表数据作为value.
 * 现仅支持list,其他数据类型也留好了实现位置,在同包的对应js中.
 * date:17/5/8
 * @author luckyqqk
 * @param {object} redis
 */
var GameRedis = function(redis){
    this.redis = redis;
    this.dataType = {};
    this.dataType['list'] = require('./list');
    this.dataType['hash'] = require('./hash');
    this.dataType['set'] = require('./set');
    this.dataType['zset'] = require('./zset');
};
module.exports = GameRedis;

var REDIS_TYPE  = "redisType";      // 以何数据格式存入redis
var PRIMARY     = 'primary';
var FOREIGN     = 'foreign';
var SON_TABLES  = "sons";

/**
 * 缓存数据
 * @param {object}          structure           表结构信息
 * @param {string}          structure.redisType redis结构名
 * @param {string}          structure.primary   表主键
 * @param {string}          structure.foreign   表外键
 * @param {(object|array)}  toSet       将要设置缓存的数据(单条或多条)
 * @param {number}          [expire]    缓存过期时间(秒)
 * @param cb
 */
GameRedis.prototype.addRedisCache = function(structure, toSet, expire, cb) {
    this.dataType[structure[REDIS_TYPE]].addCache(this.redis, structure, toSet, expire, cb);
};

/**
 * 获取缓存数据
 * @param {object}          structure           表结构信息
 * @param {string}          structure.redisType redis结构名
 * @param {string}          structure.primary   表主键
 * @param {string}          structure.foreign   表外键
 * @param {number}          priValue  主键不为0,则获取主键对应数据,否则获取外键对应数据
 * @param {number}          forValue
 * @param cb
 */
GameRedis.prototype.getRedisCache = function(structure, priValue, forValue, cb) {
    this.dataType[structure[REDIS_TYPE]].getCache(this.redis, structure, priValue, forValue, cb);
};

/**
 * 更新数据
 * @param {object}          structure           表结构信息
 * @param {string}          structure.redisType redis结构名
 * @param {string}          structure.primary   表主键
 * @param {string}          structure.foreign   表外键
 * @param {object}          toUpd
 * @param {number}          [expire]             缓存过期时间(秒)
 * @param cb
 */
GameRedis.prototype.updateRedisCache = function(structure, toUpd, expire, cb) {
    this.dataType[structure[REDIS_TYPE]].updCache(this.redis, structure, toUpd, expire, cb);
};

/**
 * 删除缓存
 * @param {object}          structure           表结构信息
 * @param {string}          structure.redisType redis结构名
 * @param {string}          structure.primary   表主键
 * @param {string}          structure.foreign   表外键
 * @param {number}          priValue  主键不为0,则获取主键对应数据,否则获取外键对应数据
 * @param {number}          forValue
 * @param cb
 */
GameRedis.prototype.removeRedisCache = function(structure, priValue, forValue, cb) {
    this.dataType[structure[REDIS_TYPE]].remCache(this.redis, structure, priValue, forValue, cb);
};

/**
 * 根据根表和根表主键值,删除其和其下相关的数据缓存.
 * @param {string}  fatherName  父表名
 * @param {object}  structures  相关联的所有表结构信息
 * @param {number}  priValue    主键值
 * @param {*}       [forValue]  外键值
 * @param cb
 */
GameRedis.prototype.removeCacheByFather = function(fatherName, structures, priValue, forValue, cb) {
    if (typeof forValue == 'function') {
        cb = forValue;
        forValue = '*';
    }
    //console.error(`tableName:${tableName}, primaryValue:${primaryValue}, foreignValue:${foreignValue}`);
    if (!fatherName || !priValue || !structures) {
        cb(`params is null`);
        return;
    }
    var table = structures[fatherName];
    if (!table) {
        cb(`delete cache failed:: can not find table by tableName::${fatherName}`);
        return;
    }
    var self = this,
        pipeArr = [];   // 待删除数据
    // 自身数据加入待删除
    // pipeArr.push(['del', format('%s:%s', fatherName, forValue)]);
    pipeArr.push(format('%s:%s', fatherName, forValue));
    var getSonOrder = function(tName, primaryValue, _cb) {
        var theTable = structures[tName];
        if (!theTable[SON_TABLES]) {
            _cb();
            return;
        }
        var sonNames = theTable[SON_TABLES],
            sonPipe = [],
            sonTable = null,
            sonType = null,
            sonCacheKey = "";
        sonNames.forEach((sonN)=>{
            sonTable = structures[sonN];
            sonType = self.dataType[sonTable[REDIS_TYPE]];
            // 父亲的主键是儿子的外键
            sonCacheKey = sonType.makeKey(sonN, sonTable[FOREIGN], 0, primaryValue);
            sonPipe.push(sonType.getCacheOrder(sonCacheKey));   // 寻找子数据
            // pipeArr.push(['del', sonCacheKey]);                 // 将子数据加入待删除(del是通用删除)
            pipeArr.push(sonCacheKey);                 // 将子数据加入待删除(del是通用删除)
        });
        // 寻找子数据
        self.redis.multi(sonPipe).exec((err, data)=>{
            if (!!err) {
                _cb();
                console.error(`sonPipe err : ${err}`);
                return;
            }
            var length = 0;         // 多少个有效的儿子
            data.forEach((aData)=>{ // aData结构 [null, [data]]
                length += aData.length;
            });
            if (length == 0) {
                _cb();
                return;
            }
            var count = 0;      // 多少个儿子完成了找孙子的任务
            var checkEnd = function() {
                ++count === length && _cb();
            };
            var sonN = "", sonPri = null;
            data.forEach((aData, idx)=>{ // aData结构 [null, [data]]
                sonN = sonNames[idx];
                sonPri = structures[sonN][PRIMARY];
                aData.forEach((aSonData)=>{
                    // 将子数据作为父数据,向下继续查找子数据
                    getSonOrder(sonN, JSON.parse(aSonData)[sonPri], checkEnd);
                });
            });
        });
    };
    getSonOrder(fatherName, priValue, ()=>{
        self.redis.del(pipeArr, cb); // 统一删除待删除数据
    });
};