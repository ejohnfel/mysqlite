TARGET = /usr/lib/python3.8

install: mysqlite.py
	@cp $< $(TARGET)
	@chmod +x $(TARGET)/$<

actions:
	printf "install\tInstall Module\n"
