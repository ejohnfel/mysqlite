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
	def CreateTables(self,table_specs,cursor=None):
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
			DbgMsg("Opening With table_specs")

		try:
			self.ActiveConnection = sql.connect(self.DatabaseURL,detect_types=sql.PARSE_DECLTYPES|sql.PARSE_COLNAMES)

			if table_specs and len(table_specs) > 0:
				if self.DatabaseURL == ":memory:" or not os.path.exists(self.DatabaseURL) or os.path.getsize(url) == 0:
					self.CreateTables(table_specs)
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
		elif self.Cursor == None:
			self.Cursor = cursor = self.ActiveConnection.cursor()
		else:
			cursor = self.Cursor

		return cursor

	# Basic Execution Atom
	def Execute(self,cmd,parameters=None,cursor=None):
		"""Basic Execution Atom"""

		if cursor == None:
			cursor = self.GetCursor()

		results = None

		if parameters:
			results = cursor.execute(cmd,parameters)
		else:
			results = cursor.execute(cmd)

		return results

	# Execution with Result set
	def Resultset(self,cmd,parameters=None,cursor=None):
		"""Execution with result set"""

		if cursor == None:
			cursor = self.GetCursor()

		results = None

		if parameters:
			results = cursor.execute(cmd,parameters)
		else:
			results = cursor.execute(cmd)

		result_set = cursor.fetchall()

		return result_set

	# Commit
	def Commit(self):
		"""Commit"""

		if self.ActiveConnection:
			self.ActiveConnection.commit()

	# Basic Insert
	def Insert(self,cmd,parameters=None,cursor=None):
		"""Basic/Compact Insert"""

		if re.search("^insert into",cmd,re.IGNORECASE) == None:
			cmd = "INSERT INTO " + cmd

		results = None

		try:
			results = self.Execute(cmd,parameters,cursor)

			self.Commit()
		except Exception as err:
			raise err

		return results

	# Run A Basic Select
	def Select(self,cmd,parameters=None,cursor=None):
		"""Compact Select Statement"""

		if re.search("^select",cmd,re.IGNORECASE) == None:
			cmd = "SELECT " + cmd

		return self.ResultSet(cmd,parameters,cursor)

	# Update Record(s)
	def Update(self,cmd,parameters=None,cursor=None):
		"""Update Record(s)"""

		if re.search("^update",cmd,re.IGNORECASE) == None:
			cmd = "UPDATE " + cmd

		results = None

		try:
			results = self.Execute(cmd,parameters,cursor)

			self.Commit()
		except Exception as err:
			raise err

		return results

	# Delete Record(s)
	def Delete(self,cmd,parameters=None,cursor=None):
		"""Delete Record(s)"""

		if re.search("^delete from",cmd,re.IGNORECASE) == None:
			cmd = "DELETE FROM " + cmd

		results = None

		try:
			results = self.Execute(cmd,parameters,cursor)

			self.Commit()
		except Exception as err:
			raise err

		return results

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
