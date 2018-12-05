# semantic-DA
A data access layer based on Semantic-Transact.

Semantic-DA can emit sql AST building / traveling events that let users have chances to organize updating data to inject the business semantics.

The semantics is abstracted into a few patterns, which is handled by the implementation of a interface. This interface define the events that the DA layer firing.

The final data is used to build the SQL statement. In this way, a typical database application's business processing are abstracted into some semantics pattern and supported automatically, with semantics configuration.

Semantic-DA is a building block of a future framework project, semantic-jserv. With semantic-DA, typical CRUD semantics handling should sported via semantics patterns.
