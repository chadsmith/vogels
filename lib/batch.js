'use strict';

var _ = require('lodash'),
    async = require('async');

var internals = {};

internals.omitNulls = function (data) {
  return _.omitBy(data, function(value) {
    return _.isNull(value) ||
      _.isUndefined(value) ||
      (_.isArray(value) && _.isEmpty(value)) ||
      (_.isString(value) && _.isEmpty(value));
  });
};

internals.buildGetItemsRequest = function (tableName, keys, options) {
  var request = {};

  request[tableName] = _.merge({}, {Keys : keys}, options);

  return { RequestItems : request };
};

internals.buildPutItemsRequest = function (tableName, items, options, unprocessed) {
  var request = {};

  request[tableName] = unprocessed ? items : items.map(function(item) {
    return {
      PutRequest: { Item: item }
    };
  });

  return _.merge({}, { RequestItems : request }, options);
};

internals.buildDeleteItemsRequest = function (tableName, keys, options, unprocessed) {
  var request = {};

  request[tableName] = unprocessed ? keys : keys.map(function(key) {
    return {
      DeleteRequest: { Key: key }
    };
  });

  return _.merge({}, { RequestItems : request }, options);
};

internals.serializeKeys = function (keys, table, serializer) {
  return keys.map(function (key) {
    return serializer.buildKey(key, null, table.schema);
  });
};

internals.serializeItems = function (items, table, serializer) {
  return items.map(function (item) {
    return serializer.serializeItem(table.schema, internals.omitNulls(item), null);
  });
};

internals.mergeResponses = function (tableName, responses) {
  var base = {
    Responses : {},
    ConsumedCapacity : []
  };

  base.Responses[tableName] = [];

  return responses.reduce(function (memo, resp) {
    if(resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
};

internals.paginatedGet = function (request, table, options, callback) {
  var responses = [];
  var tableName = table.tableName();

  var doFunc = function (callback) {

    table.runBatchGetItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = null;

      if (_.isObject(resp.UnprocessedKeys)) {
        if (resp.UnprocessedKeys[tableName] && _.isArray(resp.UnprocessedKeys[tableName].Keys)) {
          request = internals.buildGetItemsRequest(tableName, resp.UnprocessedKeys[tableName].Keys, options);
        }
      }

      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(tableName, responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.paginatedPut = function (request, table, options, callback) {
  var responses = [];
  var tableName = table.tableName();

  var doFunc = function (callback) {

    table.runBatchPutItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = null;

      if (_.isObject(resp.UnprocessedItems)) {
        if (resp.UnprocessedItems[tableName] && _.isArray(resp.UnprocessedItems[tableName])) {
          request = internals.buildPutItemsRequest(tableName, resp.UnprocessedItems[tableName], options, true);
        }
      }

      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(tableName, responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.paginatedDelete = function (request, table, options, callback) {
  var responses = [];
  var tableName = table.tableName();

  var doFunc = function (callback) {

    table.runBatchDeleteItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = null;

      if (_.isObject(resp.UnprocessedItems)) {
        if (resp.UnprocessedItems[tableName] && _.isArray(resp.UnprocessedItems[tableName])) {
          request = internals.buildDeleteItemsRequest(tableName, resp.UnprocessedItems[tableName], options, true);
        }
      }

      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(tableName, responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.buckets = function (items, limit) {
  var buckets = [];

  while( items.length ) {
    buckets.push( items.splice(0, limit) );
  }

  return buckets;
};

internals.initialBatchGetItems = function (keys, table, serializer, options, callback) {
  var serializedKeys = internals.serializeKeys(keys, table, serializer);
  var tableName = table.tableName();

  var request = internals.buildGetItemsRequest(tableName, serializedKeys, options);

  internals.paginatedGet(request, table, options, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[tableName];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(i));
    });

    return callback(null, items);
  });
};

internals.initialBatchPutItems = function (items, table, serializer, options, callback) {
  var serializedItems = internals.serializeItems(items, table, serializer);

  var request = internals.buildPutItemsRequest(table.tableName(), serializedItems, options);

  internals.paginatedPut(request, table, options, function (err) {
    if(err) {
      return callback(err);
    }

    var results = items.map(function(item) {
      return table.initItem(item);
    });

    return callback(null, results);
  });
};

internals.initialBatchDeleteItems = function (keys, table, serializer, options, callback) {
  var serializedKeys = internals.serializeKeys(keys, table, serializer);
  var tableName = table.tableName();

  var request = internals.buildDeleteItemsRequest(tableName, serializedKeys, options);

  internals.paginatedDelete(request, table, options, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[tableName];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(i));
    });

    return callback(null, items);
  });
};

internals.getItems = function (table, serializer) {

  return function (keys, options, callback) {

    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(keys), 100), function (key, callback) {
      internals.initialBatchGetItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };

};

internals.putItems = function (table, serializer) {

  return function (items, options, callback) {

    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(items), 25), function (item, callback) {
      internals.initialBatchPutItems(item, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };

};

internals.deleteItems = function (table, serializer) {

  return function (keys, options, callback) {

    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(keys), 25), function (key, callback) {
      internals.initialBatchDeleteItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };

};

module.exports = function (table, serializer) {

  return {
    getItems : internals.getItems(table, serializer),
    putItems : internals.putItems(table, serializer),
    deleteItems : internals.deleteItems(table, serializer)
  };

};
