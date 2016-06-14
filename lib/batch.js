'use strict';

var _ = require('lodash'),
    async = require('async');

var internals = {};

internals.buildInitialGetItemsRequest = function (tableName, keys, options) {
  var request = {};

  request[tableName] = _.merge({}, {Keys : keys}, options);

  return { RequestItems : request };
};

internals.buildInitialPutItemsRequest = function (tableName, items, options) {
  var request = {};

  request[tableName] = items.map(function(item) {
    return {
      PutRequest: { Item: item }
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
    return serializer.serializeItem(table.schema, item, null);
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

internals.paginatedGet = function (request, table, callback) {
  var responses = [];

  var doFunc = function (callback) {

    table.runBatchGetItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = resp.UnprocessedKeys;
      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null && !_.isEmpty(request);
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.tableName(), responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.paginatedWrite = function (request, table, callback) {
  var responses = [];

  var doFunc = function (callback) {

    table.runBatchPutItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = resp.UnprocessedItems;
      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null && !_.isEmpty(request);
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.tableName(), responses));
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

  var request = internals.buildInitialGetItemsRequest(table.tableName(), serializedKeys, options);

  internals.paginatedGet(request, table, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[table.tableName()];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(i));
    });

    return callback(null, items);
  });
};

internals.initialBatchPutItems = function (items, table, serializer, options, callback) {
  var serializedItems = internals.serializeItems(items, table, serializer);

  var request = internals.buildInitialPutItemsRequest(table.tableName(), serializedItems, options);

  internals.paginatedWrite(request, table, function (err) {
    if(err) {
      return callback(err);
    }

    var results = items.map(function(item) {
      return table.initItem(item);
    });

    return callback(null, results);
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

module.exports = function (table, serializer) {

  return {
    getItems : internals.getItems(table, serializer),
    putItems : internals.putItems(table, serializer)
  };

};
