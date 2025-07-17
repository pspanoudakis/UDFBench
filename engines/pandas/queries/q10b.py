import utils

class Q10(utils.PandasQueryBase):
    def __init__(
        self,
        pd_module,
        schema_module,
        **udf_paths: str,
    ):
        super().__init__(
            pd_module,
            schema_module,
            scalar_udf_path=udf_paths['scalar_udf_path'],
            table_udf_path=udf_paths['table_udf_path'],
        )

    def __exec__(
        self,
        dataset_path: str,
        data_format: utils.SupportedDataFormat,
        _: str
    ):
        Artifacts = self.schemas.Artifacts
        Projects = self.schemas.Projects
        ProjectsArtifacts = self.schemas.ProjectsArtifacts
        artifacts = self.__read_df__(
            dataset_path, 'artifacts',
            [
                Artifacts.id,
                Artifacts.type,
            ],
            data_format
        )
        projects = self.__read_df__(
            dataset_path, 'projects',
            [
                Projects.id,
                Projects.fundedamount,
                Projects.currency,
            ],
            data_format
        )
        projects_artifacts = self.__read_df__(
            dataset_path, 'projects_artifacts',
            [
                ProjectsArtifacts.projectid,
                ProjectsArtifacts.artifactid,
            ],
            data_format
        )
        converttoeuro = self.scalar_udfs.converttoeuro

        sub_df = artifacts.merge(
            projects_artifacts,
            left_on=Artifacts.id,
            right_on=ProjectsArtifacts.artifactid,
            suffixes=('_a', '_pa')
        ).merge(
            projects[projects[Projects.fundedamount] > 0.0],
            left_on=f'{ProjectsArtifacts.projectid}_pa',
            right_on=Projects.id,
            suffixes=('_apa', '_p')
        )
        sub_df = self.pd.concat([
            sub_df[f'{Artifacts.id}_a'],
            sub_df[Artifacts.type],
            sub_df[[Projects.fundedamount, Projects.currency]].apply(
                lambda row: converttoeuro(
                    row[Projects.fundedamount],
                    row[Projects.currency]
                ),
                axis=1
            )
        ], axis=1)
        sub_df.columns = ('id', 'type', 'fundedamount')
        sub_df.reset_index(inplace=True)
        
        groupby_cols = ['id', 'type']
        return sub_df.groupby(by=groupby_cols)[[
            *groupby_cols, 'fundedamount'
        ]].apply(lambda itf: self.pd.Series({
            'id': itf['id'].iloc[0],
            'type': itf['type'].iloc[0],
            'fundedamount': itf['fundedamount'].sum(min_count=1),
        })).reset_index(
            level=[i for i in range(len(groupby_cols))], drop=True
        ).pipe(
            self.table_udfs.kmeans_iterative,
            group_by_column='type',
            kmeans_column='fundedamount',
            ids_column='id',
            num_clusters=5,
            pd=self.pd,
        )
