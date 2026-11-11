# Historical data setup

Download the Brazilian E-Commerce Public Dataset from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce using your own account. Respect the dataset license. Extract the nine CSV files into `data/real/`, directly under that directory, without renaming them.

The fixture demo needs no external dataset. For the full baseline run `./scripts/final.ps1 demo-real`; the source inventory and checksums are produced by the pipeline. Keep these inputs out of Git. Run both the fixture and historical demos before running the correction, CDC and dbt verification sequence in the runbook.
