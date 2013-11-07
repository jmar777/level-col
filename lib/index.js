var levelup = require('levelup'),
	async = require('async');

var sep = '\xff';

function ColumnStore(filename) {
	this.db = levelup(filename);
}

/**
 * Splits the object into "columns" and performs a batch insert.
 * Column values are stored at '<col-name><sep><key>'.
 */
ColumnStore.prototype.put = function put(key, obj, cb) {
	// todo: would it be useful to store a list of all known columns for a key
	// (e.g., at '<sep>columns<sep><key>')?
	// todo: can't store null/undefined values. need proper error message.
	
	var keySuffix = sep + key,
		cols = Object.keys(obj);

	var ops = cols.map(function(col) {
		return {
			type: 'put',
			key: col + keySuffix,
			value: obj[col]
		};
	});

	this.db.batch(ops, cb);
};

/**
 * Retrieves the requested column values stored at the provided key.
 */
ColumnStore.prototype.get = function get(key, columns, cb) {
	// todo: levelup doesn't have snapshots yet, so there's no way to do a
	// consistent multi-get against all the columns. Perhaps we can implement
	// consistency here at the code level.
	
	var self = this,
		keySuffix = sep + key;

	// make sure columns is an array
	Array.isArray(columns) || (columns = [columns]);

	var columnKeys = columns.map(function(col) {
		return col + keySuffix;
	});

	var getColumnValue = function getColumnValue(col, cb) {
		self.db.get(col, cb);
	};

	async.map(columnKeys, getColumnValue, function(err, results) {
		if (err) return cb(err);

		var ret = {};
		for (var i = 0, len = columns.length; i < len; i++) {
			ret[columns[i]] = results[i];
		}

		cb(null, ret);
	});
};

/**
 * Retrieves a readable stream of the requested column values that exist within
 * the provided startKey and stopKey range.  Results are in the form of:
 * [
 * 	 { key: 'key1', data: { col1: 'value1', col2: 'value2' } },
 * 	 { key: 'key2', data: { col1: 'value1', col2: 'value2' } },
 * 	 ...
 * ]
 */
ColumnStore.prototype.getRangeStream = function(startKey, stopKey, columns) {

};

module.exports = ColumnStore;