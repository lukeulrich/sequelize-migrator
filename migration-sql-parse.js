'use strict'

// Constants
const kDefaultDelimiter = '\n-- MIGRATION DOWN SQL\n'

/**
 * @param {String} migrationSql
 * @param {String} [optDelimiter='\n-- MIGRATION DOWN SQL\n']
 * @returns {Object.<{up, down}}
 */
module.exports = function(migrationSql, optDelimiter = kDefaultDelimiter) {
	if (!migrationSql)
		throw new Error('Missing / empty migration SQL argument')

	if (typeof migrationSql !== 'string')
		throw new Error(`migration SQL must be string; got ${typeof migrationSql} instead`)

	let delimiter = optDelimiter || exports.kDefaultDelimiter,
		delimiterPosition = migrationSql.indexOf(delimiter),
		upSql = null,
		downSql = null

	if (delimiterPosition >= 0) {
		upSql = migrationSql.substr(0, delimiterPosition)
		downSql = migrationSql.substr(delimiterPosition)
	}
	else {
		upSql = migrationSql
	}

	return {
		up: upSql,
		down: downSql
	}
}
module.exports.kDefaultDelimiter = kDefaultDelimiter
