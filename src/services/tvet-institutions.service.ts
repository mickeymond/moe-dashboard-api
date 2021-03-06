import { /* inject, */ BindingScope, inject, injectable} from '@loopback/core';
import {HttpErrors} from '@loopback/rest';
import {MssqldbDataSource} from '../datasources';

@injectable({scope: BindingScope.TRANSIENT})
export class TvetInstitutionsService {
  constructor(
    @inject('datasources.mssqldb') private mssqldbDataSource: MssqldbDataSource
  ) { }

  async getNational(year: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [dbYear]: {
          "Main": {
            "Urban": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[INSTITUTION_DATA]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_LOCALITY]=2`
            ))[0].TotalCount,
            "Rural": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[INSTITUTION_DATA]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_LOCALITY]=1`
            ))[0].TotalCount,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[INSTITUTION]
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
              "Urban": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${region.CODE_ZONE} AND [CODE_TYPE_LOCALITY]=2`
              ))[0].TotalCount,
              "Rural": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${region.CODE_ZONE} AND [CODE_TYPE_LOCALITY]=1`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
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
              "Urban": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [CODE_TYPE_LOCALITY]=2`
              ))[0].TotalCount,
              "Rural": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [CODE_TYPE_LOCALITY]=1`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
              ))[0].TotalCount
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
        [dbYear]: {
          "Main": {
            "RegionId": regionID,
            "Urban": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${regionID} AND [CODE_TYPE_LOCALITY]=2`
            ))[0].TotalCount,
            "Rural": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [RegCode]=${regionID} AND [CODE_TYPE_LOCALITY]=1`
            ))[0].TotalCount,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
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
              "Urban": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_LOCALITY]=2`
              ))[0].TotalCount,
              "Rural": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${district.DstCode} AND [CODE_TYPE_LOCALITY]=1`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
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
              "Urban": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [CODE_TYPE_LOCALITY]=2 AND [RegCode]=${regionID}`
              ))[0].TotalCount,
              "Rural": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [CODE_TYPE_LOCALITY]=1 AND [RegCode]=${regionID}`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [RegCode]=${regionID}`
              ))[0].TotalCount
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
        [dbYear]: {
          "Main": {
            "DistrictId": districtID,
            "Urban": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${districtID} AND [CODE_TYPE_LOCALITY]=2`
            ))[0].TotalCount,
            "Rural": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
              WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [DstCode]=${districtID} AND [CODE_TYPE_LOCALITY]=1`
            ))[0].TotalCount,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT COUNT(*) AS TotalCount
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
              INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
              ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
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
              "Urban": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [CODE_TYPE_LOCALITY]=2 AND [DstCode]=${districtID}`
              ))[0].TotalCount,
              "Rural": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [CODE_TYPE_LOCALITY]=1 AND [DstCode]=${districtID}`
              ))[0].TotalCount,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT COUNT(*) AS TotalCount
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION_DATA]
                ON [${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[INSTITUTION]
                ON [${dbYear}].[dbo].[INSTITUTION_DATA].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION].[CODE_INSTITUTION]
                WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND [DstCode]=${districtID}`
              ))[0].TotalCount
            }
          }))
        }
      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }
}
