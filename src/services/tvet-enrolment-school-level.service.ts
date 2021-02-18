import { /* inject, */ BindingScope, inject, injectable} from '@loopback/core';
import {HttpErrors} from '@loopback/rest';
import {MssqldbDataSource} from '../datasources';

@injectable({scope: BindingScope.TRANSIENT})
export class TvetEnrolmentSchoolLevelService {
  constructor(
    @inject('datasources.mssqldb') private mssqldbDataSource: MssqldbDataSource
  ) { }

  async getNational(year: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [dbYear]: {
          "Main": {
            "Male": (await this.mssqldbDataSource.execute(
              `SELECT ISNULL(SUM([MALE]),0) as Total
              FROM [${dbYear}].[dbo].[ENROLMENT_TVET]`
            ))[0].Total,
            "Female": (await this.mssqldbDataSource.execute(
              `SELECT ISNULL(SUM([FEMALE]),0) as Total
              FROM [${dbYear}].[dbo].[ENROLMENT_TVET]`
            ))[0].Total,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
              FROM [${dbYear}].[dbo].[ENROLMENT_TVET]`
            ))[0].Total
          },
          "LevelOne": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT [CODE_ZONE] ,[DESCRIPTION_ZONE]
            FROM [${dbYear}].[dbo].[ZONES]
            WHERE [CODE_TYPE_ZONE]=1`
          )).map(async (region: any) => {
            return {
              "Region": region.DESCRIPTION_ZONE,
              "RegionID": region.CODE_ZONE,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([MALE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                WHERE [RegCode]=${region.CODE_ZONE}`
              ))[0].Total,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([FEMALE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                WHERE [RegCode]=${region.CODE_ZONE}`
              ))[0].Total,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                WHERE [RegCode]=${region.CODE_ZONE}`
              ))[0].Total
            }
          })),
          "LevelTwo": [
            {
              "GES": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "Other Public Institutions": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "Private Institutions": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
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

  async getRegional(year: number, regionID: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [dbYear]: {
          "Main": {
            "RegionID": regionID,
            "Male": (await this.mssqldbDataSource.execute(
              `SELECT ISNULL(SUM([MALE]),0) as Total
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
              WHERE [RegCode]=${regionID}`
            ))[0].Total,
            "Female": (await this.mssqldbDataSource.execute(
              `SELECT ISNULL(SUM([FEMALE]),0) as Total
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
              WHERE [RegCode]=${regionID}`
            ))[0].Total,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
              WHERE [RegCode]=${regionID}`
            ))[0].Total
          },
          "LevelOne": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT TOP (100) [RegCode], [Region], [DstCode], [District]
            FROM [${dbYear}].[dbo].[RegDst_Inst]
            WHERE [RegCode]=${regionID}`
          )).map(async (district: any) => {
            return {
              "District": district.District,
              "DistrictID": district.DstCode,
              "Male": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([MALE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                WHERE [DstCode]=${district.DstCode}`
              ))[0].Total,
              "Female": (await this.mssqldbDataSource.execute(
                `SELECT ISNULL(SUM([FEMALE]),0) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                WHERE [DstCode]=${district.DstCode}`
              ))[0].Total,
              "Value": (await this.mssqldbDataSource.execute(
                `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                FROM [${dbYear}].[dbo].[RegDst_Inst]
                INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                WHERE [DstCode]=${district.DstCode}`
              ))[0].Total
            }
          })),
          "LevelTwo": [
            {
              "GES": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "Other Public Institutions": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "Private Institutions": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
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
        [dbYear]: {
          "Main": {
            "DistrictID": districtID,
            "Male": (await this.mssqldbDataSource.execute(
              `SELECT ISNULL(SUM([MALE]),0) as Total
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
              WHERE [DstCode]=${districtID}`
            ))[0].Total,
            "Female": (await this.mssqldbDataSource.execute(
              `SELECT ISNULL(SUM([FEMALE]),0) as Total
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
              WHERE [DstCode]=${districtID}`
            ))[0].Total,
            "Value": (await this.mssqldbDataSource.execute(
              `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
              FROM [${dbYear}].[dbo].[RegDst_Inst]
              INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
              ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
              WHERE [DstCode]=${districtID}`
            ))[0].Total
          },
          "LevelOne": [],
          "LevelTwo": [
            {
              "GES": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "Other Public Institutions": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "Private Institutions": [
                {
                  "Proficiency": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Certificate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Intermediate": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "Advance": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
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
}
