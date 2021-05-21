sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"testdb"'?ssl=false' --username 'postgres' --password '1234' --table 'journal' --target-dir '/user/root/journal' -m 1
sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"testdb"'?ssl=false' --username 'postgres' --password '1234' --table 'publication' --target-dir '/user/root/publication' -m 1
