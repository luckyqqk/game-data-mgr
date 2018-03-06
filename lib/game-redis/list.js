var List = module.exports;

List.getCacheOrder = function(key) {
    return ['lrange', key, 0, -1];
};

List.makeKey = function(tableName, foreign, priValue, forValue) {
    if (!!foreign)
        return tableName + ":" + forValue;
    else
        return tableName + ":" + priValue;
};

List.getCache = function(redis, structure, priValue, forValue, cb) {
    var key = List.makeKey(structure.name, structure.foreign, priValue, forValue);
    redis.lrange(key, 0, - 1, (err, data)=>{
        if (!!err || !data || data.length < 1) {
            cb(err);
            return;
        }
        var singleRes = null, result = [];
        data.forEach(aData=>{
            aData = JSON.parse(aData);
            result.push(aData);
            if (priValue == aData[structure.primary])
                singleRes = aData;
        });
        if (!!priValue) {
            cb(null, singleRes);
        } else {
            cb(null, result);
        }
    });
};

List.addCache = function(redis, structure, data, expire, cb) {
    if (typeof expire == 'function') {
        cb = expire;
        expire = 0;
    }
    var aValue = data;
    var forCache = [];
    if (Array.isArray(data)) {
        aValue = data[0];
        data.forEach(d=>{
            forCache.push(JSON.stringify(d));
        })
    } else {
        forCache = JSON.stringify(data);
    }
    var key = List.makeKey(structure.name, structure.foreign, aValue[structure.primary], aValue[structure.foreign]);
    redis.rpush(key, forCache, (err, res)=>{
        if (!!expire)
            redis.expire(key, expire, cb);
        else
            cb(err, res);
    });
};

List.updCache = function(redis, structure, json, expire, cb) {
    if (typeof expire == 'function') {
        cb = expire;
        expire = 0;
    }
    var key = List.makeKey(structure.name, structure.foreign, json[structure.primary], json[structure.foreign]);
    redis.lrange(key, 0, -1, (err, data)=>{
        if (!!err) {
            cb(err);
        } else if (!data || data.length < 1) {
            cb();
        } else {
            var idx = -1;
            for (let i = 0; i < data.length; i++) {
                var dataObj = JSON.parse(data[i]);
                if (json[structure.primary] == dataObj[structure.primary]) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                if (!!expire)
                    redis.expire(key, expire, cb);
                else
                    cb();
            } else {
                redis.lset(key, idx, JSON.stringify(json), (err, res)=>{
                    if (!!expire)
                        redis.expire(key, expire, cb);
                    else
                        cb(err, res);
                });
            }
        }
    });
};

List.remCache = function(redis, structure, priValue, forValue, cb) {
    var key = List.makeKey(structure.name, structure.foreign, priValue, forValue);
    if (!priValue) {
        redis.del(key, cb);
        return;
    }
    redis.lrange(key, 0, -1, (err, data)=>{
        if (!!err || !data || data.length < 1) {
            cb(`table:${structure.name} has no cache ${key}`);
            return;
        }
        var forDel = null;
        for (let i in data) {
            if (priValue == JSON.parse(data[i])[structure.primary]) {
                forDel = data[i];
                break;
            }
        }
        if (!forDel) {
            cb(`table:${structure.name} has no cache ${key}`);
        } else {
            redis.lrem(key, 0, forDel, cb);
        }
    });
};