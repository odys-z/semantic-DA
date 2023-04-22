[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.odys-z/semantic.DA/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.odys-z/semantic.DA/)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# About

Semantic-DA is a JDBC data access layer based on [Semantic-Transact](https://github.com/odys-z/semantic-transact),
providing a high level DA API.

More importantly, semantic-DA also handling data integration according to configured
semantics, in config file like

~~~
    <web-app>/WEB-INF/semantics.xml
~~~

Frequently used data relationships like automatic key and it's reference are handled,
largely improved the developement efficiency.

For complete semantics types, see the [semantics type java API doc](https://odys-z.github.io/javadoc/semantic.DA/io/odysz/semantic/DASemantics.smtype.html).

# Quick Start

Semantic-DA is an important component used by semantic-*, which can't work independently. 
If you are interesting in what kind of semantics it can handle, just download or clone 
the source,

~~~
    git clone https://github.com/odys-z/semantic-DA
~~~

then import the Eclipse project, and run the test cases.

The test cases come with a sqlite3 db file and necessary config files. See files in

~~~
    <project>/src/test/res
~~~

The sqlite3 connection is configured in "connects.xml":

~~~
<t id="drvmnger" pk="id" columns="id,type,isdef,src,usr,pswd,dbg,smtcs">
	<c>
	  <id>local-sqlite</id>
	  <type>sqlite</type>
	  <isdef>true</isdef>
	  <!-- For sqlite, src = relative path from this configure file.
		  So connection string can be: jdbc:sqlite:WEB-INF/remote.db -->
	  <src>semantic-DA.db</src>
	  <usr>test</usr>
	  <pswd>test</pswd>
	  <!-- enable sql printing -->
	  <dbg>true</dbg>
	  <smtcs>src/test/res/semantics.xml</smtcs>
	</c>
</t>
~~~

Where the connection id, "local-sqlite" is the reference id in tests code.

The file configured in <smtcs/> is the semantics that can be handled by semantic-DA.

~~~
    <smtcs>src/test/res/semantics.xml</smtcs>
~~~

If you really want to have a maven project depends on semantic-DA, in pom.xml:

~~~
	<dependency>
		<groupId>io.github.odys-z</groupId>
		<artifactId>semantic.DA</artifactId>
		<version>1.0.0</version>
	</dependency>
~~~

## Extending Semantics

Semantic-DA can emit sql AST building / traveling events that let users have chances
to organize updating data to inject the business semantics.

The semantics is abstracted into a few patterns, which is handled by the implementation
of an interface. This interface defines the events that the DA layer firing.

The final data is used to build the SQL statement(s). In this way, a typical database
application's business processing are abstracted into some semantics pattern and
supported semantics plugins.

In short, semantic-transact handling sql structure, ISemantics handling data
modification, semantic-DA glue this together, based on JDBC connection(s).

TODO: Docs

## Additional Docs

The quickest way is checking the DASemantextTest test cases.
