#!/usr/bin/env python3.8

#
# mysqlite - Some dumb and dirty SQLite3 Helpers
#

import os, sys, io, re

import sqlite3 as sql
from sqlite3 import Error

import csv,json
import copy, uuid

from datetime import datetime,timedelta
import time

import py_helper as ph
from py_helper import CmdLineMode, DebugMode, DbgMsg, Msg, ErrMsg

#
# Global Variables and Constants
#

# Database URL Reference
DatabaseURL = None

# Active DB Connection
ActiveConnection = None

# Lambdas

# Commit SQL Transaction
Commit = lambda connection: connection.commit()

# Close Connection
Close = lambda connection: connection.close()

#
# Functions
#

# Set Pragmas
def SetPragma(pragma,mode,connection=None):
	"""Set DB Pragma"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::SetPragma")

	if connection == None: connection = ActiveConnection

	c = connection.cursor()

	c.execute(f"PRAGMA {pragma} = {mode}")

	Commit(connection)

	DbgMsg("Exitting mysqlite::SetPragma")

# Turn On Bulk Operations
def BulkOn(connection=None):
	"""Bulk Operations On"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::BulkOn")

	if connection == None: connection = ActiveConnection

	SetPragma(connection,"journal_mode","WAL")
	SetPragma(connection,"synchronous","NORMAL")

	DbgMsg("Exitting mysqlite::BulkOn")

# Turn off Bulk Operations
def BulkOff(connection=None):
	"""Bulk Operations Off"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::BulkOff")

	if connection == None: connection = ActiveConnection

	SetPragma(connection,"journal_mode","DELETE")
	SetPragma(connection,"synchronous","FULL")

	DbgMsg("Exitting mysqlite::BulkOff")

# Create Tables
def CreateTables(table_specs,connection=None):
	"""Create Tables In Open Database"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::CreateTables")

	if connection == None : connection = ActiveConnection

	try:
		cursor = connection.cursor()

		for table_spec in table_specs:
			cursor.execute(table_spec)

		Commit(connection)
	except Exception as err:
		ErrMsg(err,"An error occurred trying to create a table")

	DbgMsg("Exitting mysqlite::CreateTables")


# Open Database
def Open(url,table_specs=None,protect_active=False):
	"""Open Database"""

	global ActiveConnection, DatabaseURL

	DbgMsg("Entering mysqlite::Open")

	if table_specs:
		DbgMsg("With table_specs")

	connection = None

	if not protect_active:
		connection = ActiveConnection

	if connection != None:
		Close(connection)

	try:
		connection = sql.connect(url)

		if table_specs and len(table_specs) > 0:
			if url == ":memory:" or not os.path.exists(url) or os.path.getsize(url) == 0:
				CreateTables(table_specs,connection=connection)

		if not protect_active:
			ActiveConnection = connection
			DatabaseURL = url

	except Error as dberr:
		ErrMsg(dberr,f"An error occurred trying to open {url}")
	except Exception as err:
		ErrMsg(err,f"An error occurred trying to open {url}")

	DbgMsg("Exitting mysqlite::Open")

	return connection

# Basic Select
def Select(statement,parameters=None,connection=None):
	"""Basic Select"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::Select")

	if connection == None: connection = ActiveConnection

	cursor = connection.cursor()

	if parameters:
		cursor.execute(statement,parameters)
	else:
		cursor.execute(statement)

	results = cursor.fetchall()

	DbgMsg("Exitting mysqlite::Select")

	return results

# Basic Execution With Commit
def __BasicExecuteWithCommit__(statement,parameters=None,connection=None):
	"""Base Function for Execution With Commit"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::__BasicExecuteWithCommit__")

	if connection == None: connection = ActiveConnection

	cursor = connection.cursor()

	result = None

	if parameters:
		result = cursor.execute(statement,parameters)
	else:
		result = cursor.execute(statement)

	Commit(connection)

	DbgMsg("Exitting mysqlite::__BasicExecuteWithCommit__")

	return result

# Basic Insert
def Insert(statement,parameters=None,connection=None):
	"""Basic Insert"""

	DbgMsg("Entering mysqlite::Insert")

	result = __BasicExecuteWithCommit__(statement,parameters,connection)

	DbgMsg("Exitting mysqlite::Insert")

	return result

# Basic Update
def Update(statement,parameters=None,connection=None):
	"""Basic Update"""

	DbgMsg("Entering mysqlite::Update")

	result = __BasicExecuteWithCommit__(statement,parameters,connection)

	DbgMsg("Exitting mysqlite::Update")

	return result

# Basic Delete
def Delete(statement,parameters=None,connection=None):
	"""Basic Delete"""

	DbgMsg("Entering mysqlite::Delete")

	result = __BasicExecuteWithCommit__(statement,parameters,connection)

	DbgMsg("Exitting mysqlite::Delete")

	return result

#
# Main Loop
#

if __name__ == "__main__":
	Msg("This module was not meant to be executed as a stand alone script")
