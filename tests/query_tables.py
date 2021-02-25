def list_datasets():
    client = bigquery.Client(credentials=credentials(), project="basedosdados")

    datasets = client.list_datasets()

    dataset_ids = [dataset.dataset_id for dataset in datasets]

    return dataset_ids


def list_tables(dataset_id):
    client = bigquery.Client(credentials=credentials(), project="basedosdados")

    dataset = client.get_dataset(dataset_id)

    tables = list(client.list_tables(dataset))

    tb_ids = [table.table_id for table in tables]

    return tb_ids


def list_all_tables():
    client = bigquery.Client(credentials=credentials(), project="basedosdados")
    datasets = list_datasets()
    tables = [
        (dataset_id, table.table_id)
        for dataset_id in datasets
        for table in list(client.list_tables(dataset_id))
    ]

    return tables


def query_all_tables():

    tabelas = list_all_tables()

    queried_tables = []

    error = {}

    for dataset_id, table_id in tabelas:
        try:
            queried_tables.append(
                [
                    f"{dataset_id}.{table_id}",
                    bd.read_table(
                        dataset_id,
                        table_id,
                        billing_project_id="basedosdados-dev",
                        limit=10,
                    ),
                ]
            )

        except Exception as err:
            error[f"{dataset_id}.{table_id}"] = (type(err), err)

    return queried_tables, error


def query_all_tables_bq():

    client = bigquery.Client(credentials=credentials(), project="basedosdados")

    queried_tables = []

    error = {}

    tables = list_all_tables()

    for dataset_id, table_id in tables:

        query = f"""SELECT * FROM basedosdados.{dataset_id}.{table_id} LIMIT 10"""

        try:
            queried_tables.append([f"{dataset_id}.{table_id}", client.query(query)])
        except Exception as err:
            error[f"{dataset_id}.{table_id}"] = (type(err), err)

    return queried_tables, error
