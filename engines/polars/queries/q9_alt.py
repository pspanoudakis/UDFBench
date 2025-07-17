import utils
import polars as pl

class Q9Alt(utils.PolarsQueryBase):
    def __init__(
        self,
        schema_module,
        **udf_paths: str,
    ):
        super().__init__(
            schema_module,
            scalar_udf_path=udf_paths['scalar_udf_path'],
            aggr_udf_path=udf_paths['aggr_udf_path'],
            table_udf_path=udf_paths['table_udf_path'],
        )

    def __exec__(
        self,
        dataset_path: str,
        data_format: utils.SupportedDataFormat,
        _: str
    ):
        return self.__scan_df__(
            dataset_path, 'artifact_authorlists', data_format,
            self.schemas.ArtifactAuthorlists, (
                'authorlist',
            ),
        ).with_columns(
            pl.col('authorlist').map_elements(
                self.scalar_udfs.jsoncount,
                return_dtype=pl.Int64,
            ).alias('json_count'),
        ).filter(
            pl.col('json_count').is_not_null() & (pl.col('json_count') <= 50)
        ).select(
            pl.col('authorlist').map_elements(
                lambda al: self.table_udfs.combinations(
                    self.scalar_udfs.clean(al), 2
                ),
                return_dtype=pl.List(pl.String),
            ).explode().drop_nulls().map_batches(
                self.aggr_udfs.aggregate_count,
                return_dtype=pl.Int64,
                returns_scalar=True,
            )
        ).collect()
