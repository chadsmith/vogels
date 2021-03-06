'use strict';

var _            = require('lodash'),
    util         = require('util'),
    AWS          = require('aws-sdk'),
    Bluebird     = require('bluebird'),
    DocClient    = AWS.DynamoDB.DocumentClient,
    Table        = require('./table'),
    Schema       = require('./schema'),
    serializer   = require('./serializer'),
    batch        = require('./batch'),
    Item         = require('./item'),
    createTables = require('./createTables');

var vogels = module.exports;

vogels.AWS = AWS;

var internals = {};

vogels.dynamoDriver = internals.dynamoDriver = function (driver) {
  if(driver) {
    internals.dynamodb = driver;

    var docClient = internals.loadDocClient(driver);
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.dynamodb = internals.dynamodb || new vogels.AWS.DynamoDB({apiVersion: '2012-08-10'});
  }

  return internals.dynamodb;
};

vogels.documentClient = internals.documentClient = function (docClient) {
  if(docClient) {
    internals.docClient = docClient;
    internals.dynamodb = docClient.service;
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.loadDocClient();
  }

  return internals.docClient;
};

internals.updateDynamoDBDocClientForAllModels = function (docClient) {
  _.each(vogels.models, function (model) {
    model.config({docClient: docClient});
  });
};

internals.loadDocClient = function (driver) {
  if(driver) {
    internals.docClient = new DocClient({service : driver});
  } else {
    internals.docClient = internals.docClient || new DocClient({service : internals.dynamoDriver()});
  }

  return internals.docClient;
};

internals.compileModel = function (name, schema) {

  // extremly simple table names
  var tableName = name.toLowerCase() + 's';

  var log = vogels.log;

  var table = new Table(tableName, schema, serializer, internals.loadDocClient(), log);

  var Model = function (attrs) {
    Item.call(this, attrs, table);
  };

  util.inherits(Model, Item);

  Model.setLogger = _.bind(table.setLogger, table);

  Model.get          = _.bind(table.get, table);
  Model.create       = _.bind(table.create, table);
  Model.update       = _.bind(table.update, table);
  Model.destroy      = _.bind(table.destroy, table);
  Model.query        = _.bind(table.query, table);
  Model.scan         = _.bind(table.scan, table);
  Model.parallelScan = _.bind(table.parallelScan, table);

  Model.getItems = batch(table, serializer).getItems;
  Model.batchGetItems = batch(table, serializer).getItems;
  Model.putItems = batch(table, serializer).putItems;
  Model.batchPutItems = batch(table, serializer).putItems;
  Model.deleteItems = batch(table, serializer).deleteItems;
  Model.batchDeleteItems = batch(table, serializer).deleteItems;

  // table ddl methods
  Model.createTable   = _.bind(table.createTable, table);
  Model.updateTable   = _.bind(table.updateTable, table);
  Model.describeTable = _.bind(table.describeTable, table);
  Model.deleteTable   = _.bind(table.deleteTable, table);
  Model.tableName     = _.bind(table.tableName, table);

  // async methods
  Model.getAsync = Bluebird.promisify(Model.get);
  Model.createAsync = Bluebird.promisify(Model.create);
  Model.updateAsync = Bluebird.promisify(Model.update);
  Model.destroyAsync = Bluebird.promisify(Model.destroy);
  Model.getItemsAsync = Bluebird.promisify(Model.getItems);
  Model.batchGetItemsAsync = Bluebird.promisify(Model.getItems);
  Model.putItemsAsync = Bluebird.promisify(Model.putItems);
  Model.batchPutItemsAsync = Bluebird.promisify(Model.putItems);
  Model.deleteItemsAsync = Bluebird.promisify(Model.deleteItems);
  Model.batchDeleteItemsAsync = Bluebird.promisify(Model.deleteItems);
  Model.createTableAsync = Bluebird.promisify(Model.createTable);
  Model.updateTableAsync = Bluebird.promisify(Model.updateTable);
  Model.describeTableAsync = Bluebird.promisify(Model.describeTable);
  Model.deleteTableAsync = Bluebird.promisify(Model.deleteTable);

  table.itemFactory = Model;

  Model.log = log;
  Model.schema = schema;

  // hooks
  Model.after  = _.bind(table.after, table);
  Model.before = _.bind(table.before, table);

  /* jshint camelcase:false */
  Model.__defineGetter__('docClient', function(){
    return table.docClient;
  });

  Model.config = function(config) {
    config = config || {};

    if(config.tableName) {
      table.config.name = config.tableName;
    }

    if (config.docClient) {
      table.docClient = config.docClient;
    } else if (config.dynamodb) {
      table.docClient = new DocClient({ service : config.dynamodb});
    }

    return table.config;
  };

  return vogels.model(name, Model);
};

internals.addModel = function (name, model) {
  vogels.models[name] = model;

  return vogels.models[name];
};

vogels.reset = function () {
  vogels.models = {};
};

vogels.Set = function () {
  return internals.docClient.createSet.apply(internals.docClient, arguments);
};

vogels.define = function (modelName, config) {
  if(_.isFunction(config)) {
    throw new Error('define no longer accepts schema callback, migrate to new api');
  }

  var schema = new Schema(config);

  var compiledTable = internals.compileModel(modelName, schema);

  return compiledTable;
};

vogels.model = function(name, model) {
  if(model) {
    internals.addModel(name, model);
  }

  return vogels.models[name] || null;
};

vogels.createTables = function (options, callback) {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  return createTables(vogels.models, options, callback);
};

vogels.setLogger = function (logger) {
  vogels.log = logger;
  Object.keys(vogels.models || {}).forEach(function(name) {
    vogels.models[name].setLogger(logger);
  });
};

vogels.types = Schema.types;

vogels.reset();
