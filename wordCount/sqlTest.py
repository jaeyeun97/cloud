import mysql.connector

cnx= {'host': 'group2dbinstance.cxezedslevku.eu-west-2.rds.amazonaws.com',
  'username': 'group2',
  'password': 'group2sibal',
  'db': 'sw777_CloudComputingCoursework'}

try:
	db = mysql.connector.connect(host = cnx['host'], database = cnx['db'], user = cnx['username'], password = cnx['password'])
except mysql.connector.Error as err:
	print(err)
else:
	db.close()

TABLES = {}
TABLES['words']=("CREATE TABLE words ("
		 "	frequency int NOT NULL,"
		 "	word varchar(100) NOT NULL)")
TABLES['letters']=("CREATE TABLE letters ("
			"frequency int NOT NULL."
			"letter varchar(1) NOT NULL")

INSERT = {}
INSERT['word'] = ("INSERT INTO words (frequency, word)"
			"VALUES (1. cloud), (2, sibal)")
INSERT['letter'] = ("INSERT INTO letters (frequency, letter"
			"VALUES (3, t), (4, z)")


cursor = db.cursor()

try:
	cursor.execute(TABLES['words'])
	cursor.execute(TABLES['letters'])
	cursor.execute(INSERT['word'])
	cursor.execute(INSERT['letter'])
except mysql.connector.Error as err2:
	print(err2)
else
	print("jot sibal')

cursor.close()
db.close()
