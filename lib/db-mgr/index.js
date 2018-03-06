/**
 * 数据库sql的执行
 * todo 单独抽离出来是为了将来做延迟更新以及批量更新.
 */
var DBMgr = function(pool) {
    this.pool = pool;
};
module.exports = DBMgr;

DBMgr.prototype.query = function(sql, valueArray, cb) {
    this.pool.query(sql, valueArray, cb);
};