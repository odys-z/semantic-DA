# About Testing Resources

- config.xml

The general configurations.

- connects.xml

JDBC connection configurations.

This file configured a few connects:

    (1) local-sqlite

A sqlite connection using file ./semantic-DA.db

    (2) local-mysql

A connection to mysql. If you are using a servlet container like tomcat, you need configure the connection pool.

    (3) inet

Another connection to mysql.

- datasets.xml (not used?)

A semantic-DA dataset configuration file, defining sqls for selecting data.

This file is the configuration used by DatasetCfg and SemanticTree.

- semantic-DA.db

A sqlite database file for testing all the semantics handled by DASemantics.

- Sun Yet-sen.jpg

A jpg file for test extFile semantics. It will create a sub-folder here named "uploads/[user-id]".

- synmatics.xml

- synode-[1234].db

Sqlite storage file for 4 synodes.