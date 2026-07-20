# CSCU sample blueprints

These `.ktr`/`.kjb` are **minimal migration-input blueprints**, not
Spoon-authored transformations. They contain just enough XML — the
connection, SQL and target table — for `pdi2dag` to **convert** them into
Airflow DAGs and emit **structural lineage** (`cscu_core.transactions →
staging.txn_stg`, etc.).

They deliberately omit the `<GUI>` coordinates, `<copies>` and full step
definitions a real transformation carries, so:

- **they do not render in Spoon** (blank canvas — expected), and
- **they are not executable on Carte**.

They show the *shape* of the CSCU pipeline for the migration + lineage
demo. To **run** a pipeline live, build it in Spoon (Table Input →
Table Output / Write to Log), which writes a complete executable `.ktr`,
then migrate that — see
[workshop/CSCU/CSCU-CAPSTONE.md](../../workshop/CSCU/CSCU-CAPSTONE.md)
(Module 0).
