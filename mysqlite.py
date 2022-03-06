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

import argparse

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

# Choices for command
__choices__ = [ "select", "insert", "update", "delete", "execute" ]

#
# Functions
#

# Internal Execute
def __BasicExecuteWithNoCommit__(statement,parameters=None,connection=None):
	"""Basic Execute With No Commit"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::Command")

	if connection == None: connection = ActiveConnection

	cursor.connection.cursor()

	results = None

	if parameters:
		results = cursor.execute(statement,parameters)
	else:
		results = cursor.execute(statement)

	DbgMsg("Exitting mysqlite::Command")

	return results, cursor

# Execute A Generic Statement
def Execute(statement,parameters=None,connection=None):
	"""Execute A Statement"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::Command")

	results,cursor = __BasicExecuteWithNoCommit__(statement,parameters,connection)

	DbgMsg("Exitting mysqlite::Command")

	return results,cursor

# Set Pragmas
def SetPragma(pragma,mode,connection=None):
	"""Set DB Pragma"""

	global ActiveConnection

	DbgMsg("Entering mysqlite::SetPragma")

	if connection == None: connection = ActiveConnection

	statement = f"PRAGMA {pragma} = {mode}"

	result = __BasicExecuteWithCommit__(statement,None,connection)

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
		result = __BasicExecuteWithCommit__(table_specs,None,connection)
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

	results,cursor = __BasicExecuteWithNoCommit__(statement,parameters,connection)

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
# Command Processing
#

# Process Commands Given On Command Line
def ProcessCommand(args,unknowns):
	"""Process Command Line Request"""

	global __choices__

	command = args.command
	file = args.file

	if file == None:
		Msg("To process commands you need to provide a file with '-f'")
		return

	parser = argparse.ArgumentParser(description="sqlite3 command line processor")

	subparser = parser.add_subparsers(help="Sqlite3 Command line processor",dest="command")

	cmd_select = subparser.add_parser("select",help="Select on a table [select columns table clause]")
	# select [columns] from [table] where [clause]
	cmd_select.add_argument("columns",nargs=1,help="Columns to select")
	cmd_select.add_argument("table",nargs=1,help="Table to select from")
	cmd_select.add_argument("clause",nargs="*",help="Clause Arguments")

	cmd_insert = subparser.add_parser("insert",help="Insert into a table")
	# insert into [table] ([columns]) values([values])

	cmd_update = subparser.add_parser("update",help="Update table")
	# update [table] ([columns]) values([values])

	cmd_delete = subparser.add_parser("delete",help="Delete from table")
	# delete from [table] where [clauses]

	cmd_execute = subparser.add_parser("execute",help="Execute a statement")
	# execute [statement]
	cmd_execute.add_argument("statement",nargs="?",help="Execute given statement")

	new_arguments = [ command ]
	new_arguments.extend(unknowns)

	args = parser.parse_args(new_arguments)

	connection = Open(file)

	if connection == None:
		Msg(f"Could not open {file}")
		return

	if args.command == "select":
		cmd = f"select {args.columns} from {args.table}"

		if len(args.clause) > 0:
			clause = " ".join(args.clause)
			cmd = f"{cmd} where {clause}"

		results = Select(cmd)

		for result in results:
			Msg(result)
	elif args.command == "insert":
		pass
	elif args.command == "update":
		pass
	elif args.command == "delete":
		pass
	elif args.command == "execute":
		cmd = " ".join(args.statement)

		results = Execute(cmd)

	Close()

#
# Support and Testing
#

# Build Parser
def BuildParser():
	"""Build Parser"""

	global __choices__

	parser = argparse.ArgumentParser(prog="mysqlite",description="Mysqlite Support Lib")

	parser.add_argument("-d","--debug",action="store_true",help="Enter debug mode")
	parser.add_argument("-t","--test",action="store_true",help="Run Test Stub")
	parser.add_argument("-f","--file",help="File to run commands against")
	parser.add_argument("command",nargs="?",choices=__choices__,help="Command to execute")

	return parser

# Test Stub
def Test(**kwargs):
	"""Test Stub"""

	ph.NotImplementedYet()

#
# Main Loop
#

if __name__ == "__main__":
	CmdLineMode(True)

	parser = BuildParser()

	args,unknowns = parser.parse_known_args()

	if args.debug:
		DebugMode(True)

	if args.test:
		Test(argument=args,unknowns=unknowns)
	elif args.command != None:
		ProcessCommand(args,unknowns)
	else:
		Msg("This module was not meant to be executed as a stand alone script")
