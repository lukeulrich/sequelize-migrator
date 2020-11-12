'use strict';

// Core
const assert = require('assert');
const fs = require('fs');
const path = require('path');

// Local
const migrationSqlParse = require('./migration-sql-parse');

// Constants
const kSavePointName = 'migrations',
  kDefaults = {
    pattern: /^\d{14}_\d{4}_[\w-_]+\.sql$/,
    modelName: 'MigrationMeta',
    tableName: 'migrations_meta',
  };

/**
 * @param {Array} a
 * @param {Array} b
 * @returns {Array} elements in ${a} that are not in ${b}
 */
function difference(a, b) {
  let bSet = new Set(b);
  return a.filter((x) => !bSet.has(x));
}

/**
 * Migrator facilitates running and reverting migrations safely. Migrations are stored locally on
 * the filesystem. Migrations that have been executed are stored inside a PostgreSQL database table.
 * To prevent race conditions between multiple instances attempting to run the same migrations
 * simultaneously, an exclusive lock is obtained on the migrations meta table until all migrations
 * have been processed.
 *
 * All migrations are executed within a single transaction that includes the migrations table lock.
 * If a migration fails, all previous migrations are persisted (accomplished by using SAVEPOINTs).
 *
 * Each migration should be a sql file containing both the 'up' and 'down' SQL separated by a
 * specific delimiter (by default, is -- DOWN MIGRATION SQL). For example:
 *
 * -- File: migration.sql
 * create table users (
 *     id serial primary key,
 *     name text not null,
 *     password text not null,
 *     unique(name)
 * );
 *
 * -- DOWN MIGRATION SQL
 * drop table users;
 *
 * Note: Because all migrations are run in their own transaction, it is not necessary to input begin
 * / commit clauses inside the migration SQL.
 */
module.exports =
class Migrator {
  /**
	 * @constructor
	 * @param {Object} sequelize - configured instance of the sequelize library
	 * @param {Object} options
	 * @param {String} options.path - local path where migration files are stored
	 * @param {RegExp} [options.pattern=/^\d{14}_\d{4}_[\w-_]+\.sql$/] - only consider files in ${options.path} that match this pattern as migration files
	 * @param {String} [options.modelName='MigrationMeta'] - name of sequelize model
	 * @param {String} [options.tableName='migrations_meta'] - database table where migrations are stored
	 * @param {String} [options.schema='public']
	 * @param {Object} [options.logger=null] - bunyan logger instance
	 */
  constructor(sequelize, options = {}) {
    assert(sequelize, 'sequelize argument is required');
    assert(options.path, 'options.path argument is required');

    this.sequelize_ = sequelize;
    this.migrationFilePath_ = options.path;
    this.migrationFilePattern_ = options.pattern || kDefaults.pattern;
    this.modelName_ = options.modelName || kDefaults.modelName;
    this.tableName_ = options.tableName || kDefaults.tableName;
    this.schema_ = options.schema || 'public';
    this.logger_ = options.logger;

    this.migrationFiles_ = null;
    this.model_ = this.createModel_();

    this.searchPath_ = null;
  }

  /**
	 * Runs all pending migrations. Creates the migration table if it does not already exist, and
	 * exclusively locks the migrations table so that only a single instance may run migrations at a
	 * given time.
	 *
	 * @returns {Promise.<Array.<String>>} - array of migration file names that were executed
	 */
  up() {
    return this.ensureTableExists_()
      .then(() => this.sequelize_.transaction((transaction) => {
        return this.lockTable_(transaction)
          .then(this.pending.bind(this, transaction))
          .then((pendingMigrations) => {
            let numPending = pendingMigrations.length;
            if (numPending === 0) {
              this.log_('info', 'No migrations are pending');
              return null;
            }

            this.log_('info', `${numPending} pending migration(s)`);
            return this.saveSearchPath_(transaction)
              .then(() => this.setSearchPath_(this.schema_, transaction))
              .then(() => this.migrationsUp_(pendingMigrations, transaction))
              .then((migrations) => {
                return this.setSearchPath_(this.searchPath_, transaction)
                  .then(() => {
                    this.searchPath_ = null;
                    this.log_('info', 'Migrations complete');
                    return migrations.map((migration) => migration.get('name'));
                  });
              })
              .catch((error) => {
                this.log_('fatal', {error}, `Unable to perform the last migration: ${error.message}`);
                throw error;
              });
          });
      }));
  }

  /**
	 * Undoes ${optAmount} migrations in reverse order that they were executed.
	 *
	 * @param {Number} [optAmount=1] - number of migrations to rollback
	 * @returns {Promise.<Array.<String>>}
	 */
  down(optAmount = 1) {
    return this.ensureTableExists_()
      .then(() => this.sequelize_.transaction((transaction) => {
        return this.lockTable_(transaction)
          .then(this.recentlyExecuted.bind(this, optAmount, transaction))
          .then((migrationsToUndo) => {
            let numToUndo = migrationsToUndo.length;
            if (numToUndo === 0) {
              this.log_('info', 'No migrations found');
              return null;
            }

            this.log_('info', `Preparing to undo ${numToUndo} migration(s)`);
            migrationsToUndo.reverse();
            return this.saveSearchPath_(transaction)
              .then(() => this.migrationsDown_(migrationsToUndo, transaction))
              .then((migrations) => {
                return this.setSearchPath_(this.searchPath_, transaction)
                  .then(() => {
                    this.searchPath_ = null;
                    this.log_('info', 'Rollback complete');
                    return migrations.map((migration) => migration.get('name'));
                  });
              })
              .catch((error) => {
                this.log_('fatal', {error}, `Unable to undo the last migration: ${error.message}`);
                throw error;
              });
          });
      }));
  }

  /**
	 * @returns {Promise.<Array.<String>>} - lexically sorted list of matching migration files
	 */
  migrationFiles() {
    if (this.migrationFiles_)
      return Promise.resolve(this.migrationFiles_);

    return new Promise((resolve, reject) => {
      fs.readdir(this.migrationFilePath_, 'utf8', (error, files) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(files);
      });
    }).then((files) => files.filter((x) => this.migrationFilePattern_.test(x)).sort());
  }

  /**
	 * @returns {Model} - sequelize migration model
	 */
  model() {
    return this.model_;
  }

  /**
	 * @param {Transaction} [optTransaction]
	 * @returns {Promise.<Array.<Model>>} - ordered array of migration instances that have been executed
	 */
  executed(optTransaction) {
    return this.model_.findAll({
      order: [
        ['id', 'ASC'],
      ],
      attributes: ['id', 'name'],
      transaction: optTransaction,
    });
  }

  /**
	 * @param {Number} [optAmount=1] number of transactions to undo
	 * @param {Transaction} [optTransaction]
	 * @returns {Promise.<Array.<Model>>}
	 */
  recentlyExecuted(optAmount = 1, optTransaction = null) {
    let amount = Math.max(1, optAmount);
    return this.executed(optTransaction)
      .then((executedMigrations) => executedMigrations.slice(-amount));
  }

  /**
	 * @param {Transaction} [optTransaction]
	 * @returns {Promise.<Array.<Model>>} - ordered array of migration instances that have not yet been executed
	 */
  pending(optTransaction) {
    return Promise.all([
      this.migrationFiles(),
      this.executed(optTransaction),
    ])
      .spread((migrationFiles, executedMigrations) => {
        let executedFileNames = executedMigrations.map((x) => x.name),
          pendingMigrationFileNames = difference(migrationFiles, executedFileNames);
        pendingMigrationFileNames.sort();
        let pendingMigrations = pendingMigrationFileNames.map((x) => this.model_.build({name: x}));
        return pendingMigrations;
      });
  }

  // ----------------------------------------------------
  // Private methods
  /**
	 * @returns {Model}
	 */
  createModel_() {
    let fields = {
        name: {
          type: this.sequelize_.constructor.TEXT,
          allowNull: false,
          unique: true,
        },
      },
      params = {
        tableName: this.tableName_,
        schema: this.schema_,
        charset: 'utf8',
        timestamps: true,
        updatedAt: false,
      };

    return this.sequelize_.define(this.modelName_, fields, params);
  }

  /**
	 * @returns {Promise}
	 */
  ensureTableExists_() {
    return this.model_.sync();
  }

  /**
	 * @param {Transaction} transaction
	 * @returns {Promise}
	 */
  lockTable_(transaction) {
    return this.sequelize_.query(`LOCK TABLE ${this.model_.getTableName()} IN ACCESS EXCLUSIVE MODE`,
      {raw: true, transaction});
  }

  /**
	 * Convenience method for logging a message if a logger is defined.
	 *
	 * @param {string} method the bunyan logger method call (e.g. 'info')
	 * @param {...*} params arguments to pass to the logger
	 */
  log_(method, ...params) {
    if (this.logger_)
      this.logger_[method](...params);
  }

  /**
	 * @param {Array.<String>} migrations - list of migration file names to perform
	 * @param {Transaction} transaction - sequelize transaction
	 * @returns {Promise.<Array.<Model>>} - list of executed transactions
	 */
  migrationsUp_(migrations, transaction) {
    return Promise.each(migrations, (migration) => {
      this.log_('info', {fileName: migration.name}, `  >> ${migration.name}`);

      return this.parseMigration_(migration.name)
        .then((migrationSql) => {
          let sql = migrationSql.up;
          if (!sql)
            throw new Error(`cannot run migration, ${migration.name}: missing 'up' SQL`);

          return this.savePoint_(transaction)
            .then(() => this.sequelize_.query(sql, {raw: true, transaction}))
            .catch((error) => {
              return this.rollbackToSavePoint_(transaction)
                .then(() => transaction.commit())
                .then(() => {
                  throw error;
                });
            })
            .then(() => this.releaseSavePoint_(transaction));
        })
        .then(() => migration.save({transaction}));
    })
      .then(() => migrations);
  }

  /**
	 * @param {Array.<String>} migrations - list of migration file names to perform
	 * @param {Transaction} transaction - sequelize transaction
	 * @returns {Promise.<Array.<Model>>} - list of executed transactions
	 */
  migrationsDown_(migrations, transaction) {
    return Promise.each(migrations, (migration) => {
      this.log_('info', {fileName: migration.name}, `  << Reverting ${migration.name}`);

      return this.parseMigration_(migration.name)
        .then((migrationSql) => {
          let sql = migrationSql.down;
          if (!sql)
            throw new Error(`cannot run migration, ${migration.name}: missing 'down' SQL`);

          return this.savePoint_(transaction)
            .then(() => this.sequelize_.query(sql, {raw: true, transaction}))
            .catch((error) => {
              return this.rollbackToSavePoint_(transaction)
                .then(() => transaction.commit())
                .then(() => {
                  throw error;
                });
            })
            .then(() => this.releaseSavePoint_(transaction));
        })
        .then(() => migration.destroy({transaction}));
    })
      .then(() => migrations);
  }

  /**
	 * @param {String} migrationFileName
	 * @returns {Promise.<Object>} - migration sql split into up / down SQL chunks
	 */
  parseMigration_(migrationFileName) {
    let migrationFile = path.resolve(this.migrationFilePath_, migrationFileName);
    return new Promise((resolve, reject) => {
      readFile(migrationFile, 'utf8', (error, sql) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(sql);
      })
    }).then(migrationSqlParse);
  }

  /**
	 * @param {Transaction} transaction
	 * @returns {Promise}
	 */
  savePoint_(transaction) {
    return this.sequelize_.query(`SAVEPOINT ${kSavePointName}`, {raw: true, transaction});
  }

  /**
	 * @param {Transaction} transaction
	 * @returns {Promise}
	 */
  releaseSavePoint_(transaction) {
    return this.sequelize_.query(`RELEASE SAVEPOINT ${kSavePointName}`, {raw: true, transaction});
  }

  /**
	 * @param {Transaction} transaction
	 * @returns {Promise}
	 */
  rollbackToSavePoint_(transaction) {
    return this.sequelize_.query(`ROLLBACK TO SAVEPOINT ${kSavePointName}`, {raw: true, transaction});
  }

  saveSearchPath_(transaction) {
    return this.sequelize_.query('SHOW search_path', {raw: true, plain: true, transaction})
      .then((result) => {
        this.searchPath_ = result.search_path;
      });
  }

  setSearchPath_(searchPath, transaction) {
    if (!searchPath)
      return Promise.resolve();

    this.log_('info', {searchPath}, `Setting search_path to ${searchPath}`);
    return this.sequelize_.query(`set search_path to ${searchPath}`, {raw: true, transaction});
  }
};
