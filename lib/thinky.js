'use strict';

var rethinkdbdash = require('rethinkdbdash');
var Promise = require('bluebird');
var Model = require(__dirname+'/model.js');
var util = require(__dirname+'/util.js');
var type = require(__dirname+'/type/index.js');
var Query = require(__dirname+'/query.js');
var Errors = require(__dirname+'/errors.js');

/**
 * Main method, create the default database.
 *
 * @param {Object} options the options for the driver and the future models created.
 *  - `max` {number} The maximum number of connections in the pool, default 1000
 *  - `buffer` {number} The minimum number of connections available in the pool, default 50
 *  - `timeoutError` {number} The wait time before reconnecting in case of an error (in ms), default 1000
 *  - `timeoutGb` {number} How long the pool keep a connection that hasn't been used (in ms), default 60*60*1000
 *  - `enforce_missing` {boolean}, default `false`
 *  - `enforce_extra` {"strict"|"remove"|"none"}, default `"none"`
 *  - `enforce_type` {"strict"|"loose"|"none"}, default `"loose"`
 *  - `timeFormat` {"raw"|"native"}
 */
function Thinky(config) {
  var self = this;

  self.createModelFromClass = this.createModelFromClass.bind(self)

  config = config || {};
  config.db = config.db || 'test'; // We need the default db to create it.
  self._config = config;

  self._options = {};
  // Option passed to each model we are going to create.
  self._options.enforce_missing =
    (config.enforce_missing != null) ? config.enforce_missing : false;
  self._options.enforce_extra =
    (config.enforce_extra != null) ? config.enforce_extra : "none";
  self._options.enforce_type =
    (config.enforce_type != null) ? config.enforce_type : 'loose';

  // Format of time objects returned by the database, by default we convert
  // them to JavaScript Dates.
  self._options.timeFormat =
    (config.timeFormat != null) ? config.timeFormat : 'native';
  // Option passed to each model we are going to create.
  self._options.validate =
    (config.validate != null) ? config.validate : 'onsave';

  if (config.r === undefined) {
    self.r = rethinkdbdash(config);
  }
  else {
    self.r = config.r;
  }
  self.type = type;
  self.Query = Query;
  self.models = {};

  // Export errors
  self.Errors = Errors;

  // Initialize the database.
  self.dbReady().then().error(function(error) {
    throw error;
  });
}


/**
 * Initialize our database.
 * @return {Promise=} Returns a promise which will resolve when the database is ready.
 */
Thinky.prototype.dbReady = function() {
  var self = this;
  if (this._dbReadyPromise) return this._dbReadyPromise;
  var r = self.r;
  this._dbReadyPromise = r.dbCreate(self._config.db)
  .run()
  .error(function(error) {
    // The `do` is not atomic, we a concurrent query could create the database
    // between the time `dbList` is ran and `dbCreate` is.
    if (error.message.match(/^Database `.*` already exists in/)) {
      return;
    }

    // In case something went wrong here, we do not recover and throw.
    throw error;
  });

  return self._dbReadyPromise;
};

/**
 * Return the current option used.
 * @return {object} The global options of the library
 */
Thinky.prototype.getOptions = function() {
  return this._options;
}


/**
 * Create a model
 *
 * @param {string} name The name of the table used behind this model.
 * @param {object|Type} schema The schema of this model.
 * @param {object=} options Options for this model. The fields can be:
 *  - `init` {boolean} Whether the table should be created or not. The value
 *  `false` is used to speed up testing, and should probably be `true` in
 *  other use cases.
 *  - `timeFormat` {"raw"|"native"} Format of ReQL dates.
 *  - `enforce_missing` {boolean}, default `false`.
 *  - `enforce_extra` {"strict"|"remove"|"none"}, default `"none"`.
 *  - `enforce_type` {"strict"|"loose"|"none"}, default `"loose"`.
 *  - `validate` {"oncreate"|"onsave"}, default "onsave".
 */
Thinky.prototype.createModel = function(name, schema, options) {
  var self = this;

  // Make a deep copy of the options as the model may overwrite them.
  var fullOptions = util.deepCopy(this._options);
  options = options || {};
  util.loopKeys(options, function(options, key) {
    fullOptions[key] = options[key];
  });

  // Two models cannot share the same name.
  if (self.models[name] !== undefined) {
    throw new Error("Cannot redefine a Model");
  }

  // Create the constructor returned. This will also validate the schema.
  var model = Model.new(name, schema, fullOptions, self);

  // Keep a reference of this model.
  self.models[name] = model;
  return model;
}


/**
 * Create a model from a class.
 *
 * This is useful when you want to define your models using the modern ES6
 * `class` syntax.
 *
 * You can either use this as a higher level function or a decorator.
 * @example
 * import {createModelFromClass} from './initThinky'
 *
 * // Using a decorator:
 * @createModelFromClass
 * class MyModel {
 *   static schema = {...};
 *   static index = [
 *     'id',
 *     ['path', (doc) => [doc('parent'), doc('id')]]
 *     ...
 *   ];
 *   static options = {...};  // See `createModel`
 * }
 *
 * // With a custom model name:
 * @createModelFromClass('my_model')
 * class MyModel {...}
 *
 * // Using a higher order function:
 * class MyModel {...}
 * MyModel = createModelFromClass(MyModel)
 * // custom model name:
 * MyModel = createModelFromClass(MyModel, 'my_model')
 *
 * @param {string|object} arg - Table name | Models class object
 * @param {string} name - Table name
 *
 * @return {function|object} - Curried `createModelFromClass` | Thinky Model
 */
Thinky.prototype.createModelFromClass = function(arg, clsName) {
  // If `arg` is a string, we assume it's the model name and return a
  // right-curried `createModelFromClass`
  if(typeof arg === 'string') {
    return (cls) => Thinky.prototype.createModelFromClass.call(this, cls, arg);
  }
  const cls = arg;

  const model = this.createModel(clsName || cls.name, cls.schema, cls.options);

  model.on('_docInit', (doc) => {
    util.assignPropertyDescriptors(doc, cls.prototype, {
      exclude: ['constructor']
    });
    if(doc.init) {
      // Basically an optional constructor
      doc.init();
    }
  });

  util.assignPropertyDescriptors(model, cls, {
    exclude: ['name', 'options', 'index', 'prototype', 'length']
  });

  // TODO: Either add more hooks here or create a "hook decorator"
  if(cls.prototype.preSave) {
    model.pre('save', cls.prototype.preSave);
  }

  if(cls.index) {
    for(let i in cls.index) {
      // Make sure item is list (concat trick) and apply list to `ensureIndex`
      model.ensureIndex.apply(model, [].concat(cls.index[i]));
    }
  }

  return model;
}


/**
 * Method to clean all the references to the models. This is used to speed up
 * testing and should not be used in other use cases.
 */
Thinky.prototype._clean = function() {
  this.models = {};
}


// Export the module.
module.exports = function(config) {
  return new Thinky(config);
}

// Expose thinky types directly from module
module.exports.type = type;

// Expose the Thinky class for more freedom
module.exports.Thinky = Thinky;
