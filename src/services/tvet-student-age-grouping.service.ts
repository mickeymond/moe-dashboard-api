import { /* inject, */ BindingScope, inject, injectable} from '@loopback/core';
import {HttpErrors} from '@loopback/rest';
import {MssqldbDataSource} from '../datasources';

@injectable({scope: BindingScope.TRANSIENT})
export class TvetStudentAgeGroupingService {
  constructor(
    @inject('datasources.mssqldb') private mssqldbDataSource: MssqldbDataSource
  ) { }

  async getNational(year: number) {
    const dbYear = `db${year}_Ghana`;
    try {
      return {
        [year]: {
          "Main": {
            "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
            "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
            "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
          },
          "LevelOne": [
            {
              "Region": "AHAFO",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "ASHANTI",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "BONO",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "BONO EAST",
              "RegionID": "region_id",
              "Total_Enrolments_ by_Age_Grouping": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "CENTRAL",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "EASTERN",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "GREATER ACCRA",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "NORTH EAST",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "NORTHERN",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "OTI",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "SAVANNA",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "UPPER EAST",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "UPPER WEST",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "VOLTA",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "WESTERN",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            },
            {
              "Region": "WESTERN NORTH",
              "RegionID": "region_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above for the databaseyear",
            }
          ],
          "LevelTwo": [
            {
              "GES": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "OtherPublicInstitutions": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "PrivateInstitutions": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
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
        [year]: {
          "Main": {
            "RegionID": "region_id",
            "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above in region_id for the databaseyear",
            "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above in region_id for the databaseyear",
            "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above in region_id for the databaseyear",
          },
          "LevelOne": [
            {
              "District": "DISTRICT_NAME",
              "DistrictID": "district_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above in district_id for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above in district_id for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above in district_id for the databaseyear",
            },
            {
              "District": "DISTRICT_NAME",
              "DistrictID": "district_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above in district_id for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above in district_id for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above in district_id for the databaseyear",
            },
            {
              "District": "DISTRICT_NAME",
              "DistrictID": "district_id",
              "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above in district_id for the databaseyear",
              "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above in district_id for the databaseyear",
              "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above in district_id for the databaseyear",
            }
          ],
          "LevelTwo": [
            {
              "GES": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "OtherPublicInstitutions": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "PrivateInstitutions": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
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
        [year]: {
          "Main": {
            "DistrictID": "district_id",
            "Value": "calculate_total number of enrolments for students from 14 years or less to 18 & above in district_id for the databaseyear",
            "Male": "calculate_number of enrolments for male students from 14 years or less to 18 years and above in district_id for the databaseyear",
            "Female": "calculate_number of enrolments for female students from 14 years or less to 18 years and above in district_id for the databaseyear"
          },
          "LevelOne": [],
          "LevelTwo": [
            {
              "GES": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "OtherPublicInstitutions": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ]
                }
              ],
              "PrivateInstitutions": [
                {
                  "14yrsorless": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "15yrsto17yrs": [
                    {
                      "Male": "00",
                      "Female": "00"
                    }
                  ],
                  "18yrsandabove": [
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
          }))).filter((a: any) => a.Value > 0).sort((a: any, b: any) => a.Value - b.Value).slice(0, 5)
        }
      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }
}
