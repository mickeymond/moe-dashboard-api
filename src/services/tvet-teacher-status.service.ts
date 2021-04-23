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
              "Data": {
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
                    WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([PROFESSIONAL_TEACHER] IS NOT NULL)`
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
                    WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([PROFESSIONAL_TEACHER] IS NOT NULL)`
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
                    WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([PROFESSIONAL_TEACHER] IS NOT NULL)`
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
                    WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=1 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([PROFESSIONAL_TEACHER] IS NULL)`
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
                    WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_SEX]=2 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([PROFESSIONAL_TEACHER] IS NULL)`
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
                    WHERE [CODE_TYPE_EDUCATION_SYSTEM]=3 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION} AND ([PROFESSIONAL_TEACHER] IS NULL)`
                  ))[0].TotalCount
                },
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
          "LevelOne": [
            {
              "District": "DISTRICT_NAME",
              "DistrictID": "district_id",
              "Value": "calculate_total number of Trained and Untrained TVET Teachers in district_id for the databaseyear",
              "Male": "calculate_number of Trained and Untrained Male TVET Teachers  in district_id for the databaseyear",
              "Female": "calculate_number of Trained and Untrained Female TVET Teachers in district_id for the databaseyear",
            },
            {
              "District": "DISTRICT_NAME",
              "DistrictID": "district_id",
              "Value": "calculate_total number of Trained and Untrained TVET Teachers in district_id for the databaseyear",
              "Male": "calculate_number of Trained and Untrained Male TVET Teachers  in district_id for the databaseyear",
              "Female": "calculate_number of Trained and Untrained Female TVET Teachers in district_id for the databaseyear",
            },
            {
              "District": "DISTRICT_NAME",
              "DistrictID": "district_id",
              "Value": "calculate_total number of Trained and Untrained TVET Teachers in district_id for the databaseyear",
              "Male": "calculate_number of Trained and Untrained Male TVET Teachers  in district_id for the databaseyear",
              "Female": "calculate_number of Trained and Untrained Female TVET Teachers in district_id for the databaseyear",
            }
          ],
          "LevelTwo": [
            {
              "GES": [
                {
                  "Trained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Untrained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                }
              ],
              "OtherPublicInstitutions": [
                {
                  "Trained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Untrained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    },
                  ],
                }
              ],
              "PrivateInstitutions": [
                {
                  "Trained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Untrained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                }
              ]
            }
          ]
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
            "DistrictID": "district_id",
            "Value": "calculate_total number of Trained and Untrained TVET Teachers in district_id for the databaseyear",
            "Male": "calculate_number of Trained and Untrained Male TVET Teachers  in district_id for the databaseyear",
            "Female": "calculate_number of Trained and Untrained Female TVET Teachers in district_id for the databaseyear",
          },
          "LevelOne": [],
          "LevelTwo": [
            {
              "GES": [
                {
                  "Trained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Untrained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                }
              ],
              "OtherPublicInstitutions": [
                {
                  "Trained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Untrained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                }
              ],
              "PrivateInstitutions": [
                {
                  "Trained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Untrained": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                }
              ]
            }
          ]
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
              "DistrictID": district.DstCode,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([NUMBER_MALE_AGE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_AGE_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[TYPE_AGE]
                ON [${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_TYPE_AGE]=[${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE]
                WHERE [DstCode]=${district.DstCode} AND [${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE] IN (20,21,22,23,24,25,26,27,28,29)`
              ))[0].Total,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([NUMBER_FEMALE_AGE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_AGE_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[TYPE_AGE]
                ON [${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_TYPE_AGE]=[${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE]
                WHERE [DstCode]=${district.DstCode} AND [${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE] IN (20,21,22,23,24,25,26,27,28,29)`
              ))[0].Total,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT (ISNULL(SUM([NUMBER_MALE_AGE]),0) + ISNULL(SUM([NUMBER_FEMALE_AGE]),0)) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_AGE_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[TYPE_AGE]
                ON [${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_TYPE_AGE]=[${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE]
                WHERE [DstCode]=${district.DstCode} AND [${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE] IN (20,21,22,23,24,25,26,27,28,29)`
              ))[0].Total
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
              "DistrictID": district.DstCode,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([NUMBER_MALE_AGE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_AGE_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[TYPE_AGE]
                ON [${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_TYPE_AGE]=[${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE]
                WHERE [DstCode]=${district.DstCode} AND [${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE] IN (20,21,22,23,24,25,26,27,28,29)`
              ))[0].Total,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([NUMBER_FEMALE_AGE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_AGE_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[TYPE_AGE]
                ON [${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_TYPE_AGE]=[${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE]
                WHERE [DstCode]=${district.DstCode} AND [${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE] IN (20,21,22,23,24,25,26,27,28,29)`
              ))[0].Total,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT (ISNULL(SUM([NUMBER_MALE_AGE]),0) + ISNULL(SUM([NUMBER_FEMALE_AGE]),0)) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_AGE_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_INSTITUTION]
                INNER JOIN [${dbYear}].[dbo].[TYPE_AGE]
                ON [${dbYear}].[dbo].[ENROLMENT_AGE_TVET].[CODE_TYPE_AGE]=[${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE]
                WHERE [DstCode]=${district.DstCode} AND [${dbYear}].[dbo].[TYPE_AGE].[CODE_TYPE_AGE] IN (20,21,22,23,24,25,26,27,28,29)`
              ))[0].Total
            }
          }))).filter((a: any) => a.Value > 0).sort((a: any, b: any) => a.Value - b.Value).slice(0, 5)
        }
      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }
}
