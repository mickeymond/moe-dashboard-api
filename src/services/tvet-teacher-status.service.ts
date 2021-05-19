import { /* inject, */ BindingScope, inject, injectable} from '@loopback/core';
import {HttpErrors} from '@loopback/rest';
import {MssqldbDataSource} from '../datasources';

@injectable({scope: BindingScope.TRANSIENT})
export class TvetTeacherStatusService {
  constructor(
    @inject('datasources.mssqldb') private mssqldbDataSource: MssqldbDataSource
  ) { }

  async getNational(year: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [year]: {
          "Main": {
            "Male": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1`
            ))[0].TotalCount,
            "Female": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2`
            ))[0].TotalCount,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3`
            ))[0].TotalCount
          },
          "LevelOne": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_ZONE], [DESCRIPTION_ZONE]
            FROM [${dbYear}].[dbo].[ZONES]
            WHERE [CODE_TYPE_ZONE]=1`
          )).map(async (region: any) => {
            return {
              "Region": region.DESCRIPTION_ZONE,
              "RegionId": region.CODE_ZONE,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${region.CODE_ZONE} AND [CODE_TYPE_SEX]=1`
              ))[0].TotalCount,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${region.CODE_ZONE} AND [CODE_TYPE_SEX]=2`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${region.CODE_ZONE}`
              ))[0].TotalCount
            }
          })),
          "LevelTwo": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_TYPE_TVET_INSTITUTION], [DESCRIPTION_TYPE_TVET_INSTITUTION]
            FROM [db2018_Ghana].[dbo].[TYPE_TVET_INSTITUTION]`
          )).map(async (level: any) => {
            return {
              "Level": level.DESCRIPTION_TYPE_TVET_INSTITUTION,
              "Trained": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount
              },
              "Untrained": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount
              }
            }
          }))
        }

      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }

  async getRegional(year: number, regionID: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [year]: {
          "Main": {
            "RegionID": regionID,
            "Male": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
              ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${regionID} AND [CODE_TYPE_SEX]=1`
            ))[0].TotalCount,
            "Female": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
              ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${regionID} AND [CODE_TYPE_SEX]=2`
            ))[0].TotalCount,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
              ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${regionID}`
            ))[0].TotalCount
          },
          "LevelOne": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [DstCode], [District]
            FROM [${dbYear}].[dbo].[RegDst_Inst]
            INNER JOIN [${dbYear}].[dbo].[ZONES]
            ON [${dbYear}].[dbo].[RegDst_Inst].[DstCode]=[${dbYear}].[dbo].[ZONES].[CODE_ZONE]
            WHERE [CODE_TYPE_ZONE]=2 AND [RegCode]=${regionID}`
          )).map(async (district: any) => {
            return {
              "District": district.District,
              "DistrictId": district.DstCode,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_SEX]=1`
              ))[0].TotalCount,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_SEX]=2`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode}`
              ))[0].TotalCount
            }
          })),
          "LevelTwo": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_TYPE_TVET_INSTITUTION], [DESCRIPTION_TYPE_TVET_INSTITUTION]
            FROM [db2018_Ghana].[dbo].[TYPE_TVET_INSTITUTION]`
          )).map(async (level: any) => {
            return {
              "Level": level.DESCRIPTION_TYPE_TVET_INSTITUTION,
              "Trained": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount
              },
              "Untrained": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount
              }
            }
          }))
        }
      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }

  async getDistrict(year: number, districtID: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [year]: {
          "Main": {
            "DistrictID": districtID,
            "Male": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
              ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${districtID} AND [CODE_TYPE_SEX]=1`
            ))[0].TotalCount,
            "Female": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
              ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${districtID} AND [CODE_TYPE_SEX]=2`
            ))[0].TotalCount,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[TEACHER]
              INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
              ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
              ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${districtID}`
            ))[0].TotalCount
          },
          "LevelOne": [],
          "LevelTwo": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_TYPE_TVET_INSTITUTION], [DESCRIPTION_TYPE_TVET_INSTITUTION]
            FROM [db2018_Ghana].[dbo].[TYPE_TVET_INSTITUTION]`
          )).map(async (level: any) => {
            return {
              "Level": level.DESCRIPTION_TYPE_TVET_INSTITUTION,
              "Trained": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID} AND ([YEAR_PROFESSIONAL] IS NOT NULL)`
                ))[0].TotalCount
              },
              "Untrained": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT COUNT(*) AS TotalCount
                  FROM [${dbYear}].[dbo].[TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                  ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                  ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                  INNER JOIN [db2018_Ghana].[dbo].[INSTITUTION_INFORMATION]
                  ON [db2018_Ghana].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[db2018_Ghana].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                  ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                  WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID} AND ([YEAR_PROFESSIONAL] IS NULL)`
                ))[0].TotalCount
              }
            }
          }))
        }
      }

    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }

  async getTopBottom5(year: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [year]: {
          "Top_5": (await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [DstCode], [District]
            FROM [${dbYear}].[dbo].[RegDst_Inst]
            INNER JOIN [${dbYear}].[dbo].[ZONES]
            ON [${dbYear}].[dbo].[RegDst_Inst].[DstCode]=[${dbYear}].[dbo].[ZONES].[CODE_ZONE]
            WHERE [CODE_TYPE_ZONE]=2`
          )).map(async (district: any) => {
            return {
              "District": district.District,
              "DistrictId": district.DstCode,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_SEX]=1`
              ))[0].TotalCount,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_SEX]=2`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode}`
              ))[0].TotalCount
            }
          }))).filter((a: any) => a.Value > 0).sort((a: any, b: any) => b.Value - a.Value).slice(0, 5),
          "Bottom_5": (await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [DstCode], [District]
            FROM [${dbYear}].[dbo].[RegDst_Inst]
            INNER JOIN [${dbYear}].[dbo].[ZONES]
            ON [${dbYear}].[dbo].[RegDst_Inst].[DstCode]=[${dbYear}].[dbo].[ZONES].[CODE_ZONE]
            WHERE [CODE_TYPE_ZONE]=2`
          )).map(async (district: any) => {
            return {
              "District": district.District,
              "DistrictId": district.DstCode,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_SEX]=1`
              ))[0].TotalCount,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_SEX]=2`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[TEACHER]
                INNER JOIN [${dbYear}].[dbo].[TEACHER_DATA]
                ON [${dbYear}].[dbo].[TEACHER].[ID_TEACHER]=[${dbYear}].[dbo].[TEACHER_DATA].[ID_TEACHER]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[TEACHER_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[RegDst_Inst]
                ON [${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode}`
              ))[0].TotalCount
            }
          }))).filter((a: any) => a.Value > 0).sort((a: any, b: any) => a.Value - b.Value).slice(0, 5)
        }
      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }
}
