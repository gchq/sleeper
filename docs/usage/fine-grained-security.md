Fine-grained security
=====================

Sleeper provides the tools to implement fine-grained security on the data, although further work is needed to make
these easier to use. Briefly, the following steps are required:

- Decide how to store the security information in the table, e.g. there might be one security label per row,
  or two per row, or one per cell. These fields must be added to the schema.
- Write an iterator that will run on the results of every query to filter out rows that a user is not permitted
  to see. This takes as input a user's authorisations and uses those to make a decision as to whether the user can see
  the data.
- Ensure the Sleeper instance is deployed such that the boundary of the system is protected.
- Ensure that queries are submitted to the query queue via a service that authenticates users, and passes their
  authorisations into the query time iterator configuration.
