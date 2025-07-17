import utils
import numpy as np

class Q2(utils.PandasQueryBase):
    def __init__(
        self,
        pd_module,
        schema_module,
        **udf_paths: str
    ):
        super().__init__(
            pd_module,
            schema_module,
            table_udf_path=udf_paths['table_udf_path']
        )

    def __exec__(
        self,
        dataset_path: str,
        data_format: utils.SupportedDataFormat,
        _: str
    ):
        pd = self.pd
        Artifacts = self.schemas.Artifacts
        data = self.__read_df__(
            dataset_path, 'artifacts',
            [
                Artifacts.id,
                Artifacts.date
            ],
            data_format
        )
        return pd.concat((
            data[Artifacts.id],
            data.apply(lambda d:
                self.table_udfs.extractfromdate_nonumpy(d[Artifacts.date]),
                result_type='expand', axis=1
            )
        ), axis=1)
