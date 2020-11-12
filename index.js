'use strict';

// Local
let Migrator = require('./Migrator');

module.exports = function(sequelize, options) {
  return new Migrator(sequelize, options);
};

module.exports.Migrator = Migrator;
