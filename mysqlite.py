#!/usr/bin/env python3.8

#
# mysqlite - Some dumb and dirty SQLite3 Helpers
#

import os, sys, io, re

import datetime as dt
from datetime import datetime,date,timedelta
import time

import sqlite3 as sql
from sqlite3 import Error

import csv,json
import copy,uuid

import py_helper as ph
from py_helper import CmdLineMode, DebugMode, DbgMsg, Msg, ErrMsg

import argparse

#
# Global Variables and Constants
#

# Version Numbers
VERSION=(0,0,12)
Version = __version__ = ".".join([ str(x) for x in VERSION ])

# Sqlite3 Class Wrapper
class Sqlite3Wrapper():
	"""Sqlite3 Wrapper Class"""

	# Database URL
	DatabaseURL = None
	# Active DB Connection
	ActiveConnection = None
	# Cursor
	Cursor = None

	# Initialize Instance
	def __init__(self,database_url):
		"""Initialize Instance"""

		self.DatabaseURL = database_url
		self.ActiveConnection = None
		self.Cursor = None

	# Use Database
	def Use(self,database_url):
		"""Set Database To Use"""

		if self.ActiveConnection:
			self.Close()

		self.DatabaseURL = database_url

	# Create Database
	def CreateDatabase(self,**kwargs):
		"""Create Database"""

		if self.DatabaseURL != ":memory:":
			ph.Touch(self.DatabaseURL)

		return None

	# Create Table
	def CreateTable(self,table_specs,cursor=None):
		"""Create Table"""

		result = None

		try:
			if type(table_specs) == str:
				table_specs = [ table_specs ]

			for table_spec in table_specs:
				result = self.Execute(table_spec,None,cursor)
		except Exception as err:
			raise err

		return result

	# Open Database
	def Open(self,**kwargs):
		"""Open Sqlite3 Database"""

		table_specs = kwargs.get("table_specs",None)

		if table_specs:
			DbgMsg("With table_specs")

		try:
			self.ActiveConnection = sql.connect(self.DatabaseURL,detect_types=sql.PARSE_DECLTYPES|sql.PARSE_COLNAMES)

			if table_specs and len(table_specs) > 0:
				if url == ":memory:" or not os.path.exists(url) or os.path.getsize(url) == 0:
					CreateTables(table_specs)
		except Error as dberr:
			ErrMsg(dberr,f"An error occurred trying to open {self.DatabaseURL}")
		except Exception as err:
			ErrMsg(err,f"An error occurred trying to open {self.DatabaseURL}")

		return self.ActiveConnection

	# Check if Database is Open
	def IsOpen(self):
		"""Check if Database Is Open"""

		return self.ActiveConnection != None

	# Check if Database is Closed
	def IsClosed(self):
		"""Check if Database is Closed"""

		return not self.IsOpen()

	# Close
	def Close(self):
		"""Close Open Database"""

		if self.ActiveConnection:
			pass

	# Set Pragmas
	def SetPragma(self,pragma,mode,cursor=None):
		"""Set DB Pragma"""

		statement = f"PRAGMA {pragma} = {mode}"

		result = self.Execute(statement,None,cursor)

		return result

	# Turn On Bulk Operations
	def BulkOn(self,cursor=None):
		"""Bulk Operations On"""

		self.SetPragma("journal_mode","WAL",cursor)
		self.SetPragma("synchronous","NORMAL",cursor)

	# Turn off Bulk Operations
	def BulkOff(self,cursor=None):
		"""Bulk Operations Off"""

		self.SetPragma("journal_mode","DELETE",cursor)
		self.SetPragma("synchronous","FULL",cursor)

	# Get Cursor
	def GetCursor(self,new_cursor=False):
		"""Get Current (or now) Cursor"""

		if new_cursor:
			cursor = self.ActiveConnection.cursor()
		elif self.Cursor != None:
			self.Cursor = cursor = self.ActiveConnection.cursor()
		else:
			cursor = self.Cursor

		return cursor

	# Basic Execution Atom
	def Execute(self,cmd,parameters=None,cursor=None):
		"""Basic Execution Atom"""

		if not cursor:
			cursor = self.GetCursor()

		results = None

		if parameters:
			results = cursor.execute(statement,parameters)
		else:
			results = cursor.execute(statement)

		return results

	# Execution with Result set
	def Resultset(self,cmd,parameters=None,cursor=None):
		"""Execution with result set"""

		pass

	# Commit
	def Commit(self):
		"""Commit"""

		if self.ActiveConnection:
			self.ActiveConnection.commit()

	# Basic Insert
	def Insert(self,cmd,parameters=None,cursor=None):
		"""Basic/Compact Insert"""

		try:
			self.Execute(cmd,parameters,cursor)

			self.Commit()
		except Exception as err:
			raise err

	# Run A Basic Select
	def Select(self,cmd,parameters=None,cursor=None):
		"""Compact Select Statement"""

		return self.ResultSet(cmd,parameters,cursor)

	# Update Record(s)
	def Update(self,cmd,parameters=None,cursor=None):
		"""Update Record(s)"""

		try:
			self.Execute(cmd,parameters,cursor)

			self.Commit()
		except Exception as err:
			raise err

	# Delete Record(s)
	def Delete(self,cmd,parameters=None,cursor=None):
		"""Delete Record(s)"""

		try:
			self.Execute(cmd,parameters,cursor)

			self.Commit()
		except Exception as err:
			raise err

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

	#DbgMsg("Entering mysqlite::Command")

	if connection == None: connection = ActiveConnection

	cursor = connection.cursor()

	results = None

	if parameters:
		results = cursor.execute(statement,parameters)
	else:
		results = cursor.execute(statement)

	#DbgMsg("Exitting mysqlite::Command")

	return results, cursor

# Execute A Generic Statement
def Execute(statement,parameters=None,connection=None):
	"""Execute A Statement"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::Command")

	results,cursor = __BasicExecuteWithNoCommit__(statement,parameters,connection)

	#DbgMsg("Exitting mysqlite::Command")

	return results,cursor

# Set Pragmas
def SetPragma(pragma,mode,connection=None):
	"""Set DB Pragma"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::SetPragma")

	if connection == None: connection = ActiveConnection

	statement = f"PRAGMA {pragma} = {mode}"

	result = __BasicExecuteWithCommit__(statement,None,connection)

	#DbgMsg("Exitting mysqlite::SetPragma")

	return result

# Turn On Bulk Operations
def BulkOn(connection=None):
	"""Bulk Operations On"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::BulkOn")

	if connection == None: connection = ActiveConnection

	SetPragma("journal_mode","WAL",connection)
	SetPragma("synchronous","NORMAL",connection)

	#DbgMsg("Exitting mysqlite::BulkOn")

# Turn off Bulk Operations
def BulkOff(connection=None):
	"""Bulk Operations Off"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::BulkOff")

	if connection == None: connection = ActiveConnection

	SetPragma("journal_mode","DELETE",connection)
	SetPragma("synchronous","FULL",connection)

	#DbgMsg("Exitting mysqlite::BulkOff")

# Create Tables
def CreateTables(table_specs,connection=None):
	"""Create Tables In Open Database"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::CreateTables")

	if connection == None : connection = ActiveConnection

	result = None

	try:
		if type(table_specs) == str:
			table_specs = [ table_specs ]

		for table_spec in table_specs:
			result = __BasicExecuteWithCommit__(table_spec,None,connection)
	except Exception as err:
		ErrMsg(err,"An error occurred trying to create a table")

	#DbgMsg("Exitting mysqlite::CreateTables")

	return result


# Open Database
def Open(url,table_specs=None,protect_active=False):
	"""Open Database"""

	global ActiveConnection, DatabaseURL

	#DbgMsg("Entering mysqlite::Open")

	if table_specs:
		DbgMsg("With table_specs")

	connection = None

	if not protect_active:
		connection = ActiveConnection

	if connection != None:
		Close(connection)

	try:
		connection = sql.connect(url,detect_types=sql.PARSE_DECLTYPES|sql.PARSE_COLNAMES)

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

	#DbgMsg("Exitting mysqlite::Open")

	return connection

# Basic Select
def Select(statement,parameters=None,connection=None):
	"""Basic Select"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::Select")

	if connection == None: connection = ActiveConnection

	results,cursor = __BasicExecuteWithNoCommit__(statement,parameters,connection)

	results = cursor.fetchall()

	#DbgMsg("Exitting mysqlite::Select")

	return results

# Basic Execution With Commit
def __BasicExecuteWithCommit__(statement,parameters=None,connection=None):
	"""Base Function for Execution With Commit"""

	global ActiveConnection

	#DbgMsg("Entering mysqlite::__BasicExecuteWithCommit__")

	if connection == None: connection = ActiveConnection

	cursor = connection.cursor()

	result = None

	if parameters:
		result = cursor.execute(statement,parameters)
	else:
		result = cursor.execute(statement)

	Commit(connection)

	#DbgMsg("Exitting mysqlite::__BasicExecuteWithCommit__")

	return result

# Basic Insert
def Insert(statement,parameters=None,connection=None):
	"""Basic Insert"""

	#DbgMsg("Entering mysqlite::Insert")

	result = __BasicExecuteWithCommit__(statement,parameters,connection)

	#DbgMsg("Exitting mysqlite::Insert")

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
# Support and Testing
#

# Build Parser
def BuildParser():
	"""Build Parser"""

	global __choices__

	parser = argparse.ArgumentParser(prog="mysqlite",description="Mysqlite Support Lib")

	parser.add_argument("-d","--debug",action="store_true",help="Enter debug mode")
	parser.add_argument("-t","--test",action="store_true",help="Run Test Stub")

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
	else:
		Msg("This module was not meant to be executed as a stand alone script")
