#!/usr/bin/env python3.8

import os,sys

import mysqlite3
from mysqlite3 import Sqlite3Wrapper

from datetime import date,time,datetime
import uuid

#
# Main Loop
#

if __name__ == "__main__":
	rec_id = uuid.uuid1()
	timestamp = datetime.now()

	# Fname can be :memory: or a filename, when using ":memory:" nothing is saved
	# to persistent storage.

	fname = ":memory:"

	# Instantiate Instance of Wrapper Class
	msql = Sqlite3Wrapper(fname)

	if fname != ":memory:" and not os.path.exists(fname):
		# Create Database AND tables

		# Table Creation statement, based on Sqlite3 sql syntax
		table_spec = """CREATE TABLE IF NOT EXISTS example (
				recordid VARCHAR(36),
				tag VARCHAR(64),
				description VARCHAR(1024),
				creation_date timestamp,
				modified timestamp,
				user VARCHAR(32));
				"""

		# Create Database (create zero length file)
		msql.CreateDatabase()

		# Open Database, with an implicit creation of table(s) (a list of table specs)
		msql.Open([ table_spec ])

		# Tables can also be added later using the CreateTables() member function
	else:
		# Just open database
		connection = msql.Open()

		# The call returns a connection object, but it is not needed for simple tasks
		# as the Wrapper object maintains the connection internally as "ActiveConnection"

	# Two types of statements will be demonstrated here
	# 1. A statement missing it's intended operation
	# 2. A statement with an explicit operation
	# This wrapper will detect when an operation is missing (in context to the function called)
	# and add it. You MAY, optionally provide that operation.
	# This is to retain backward comptibility with any existing sqlite3 statement code
	# and to provide a context based shortcut.

	# Insert Statement, with no operation "table_name (fields) values()"
	statement = "example (recordid,tag,description,creation_date,modified,user) values(?,?,?,?,?,?)"
	# Define Parameters
	parameters = [ str(rec_id), "A tag", "A description", timestamp, timestamp, "Me" ]

	# Execute the statement
	msql.Insert(statement,parameters)

	# An Update statement with implicit operation
	statement = "UPDATE example set description = ? where recordid = ?"
	parameters = [ "A different description", str(rec_id) ]

	msql.Update(statement,parameters)

	# Execute A select

	statement = "SELECT * from example where creation_date = ?"
	parameters = [ timestamp ]

	results = msql.Select(statement,parameters)

	for result in results:
		print(result)



