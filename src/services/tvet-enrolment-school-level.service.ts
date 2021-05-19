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
        [year]: {
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
          "LevelTwo": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_TYPE_TVET_INSTITUTION], [DESCRIPTION_TYPE_TVET_INSTITUTION]
            FROM [db2018_Ghana].[dbo].[TYPE_TVET_INSTITUTION]`
          )).map(async (level: any) => {
            return {
              "Level": level.DESCRIPTION_TYPE_TVET_INSTITUTION,
              "Proficiency": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Certificate": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Intermediate": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Advance": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[ENROLMENT_TVET]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
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
            `SELECT DISTINCT [DstCode], [District]
            FROM [${dbYear}].[dbo].[RegDst_Inst]
            INNER JOIN [${dbYear}].[dbo].[ZONES]
            ON [${dbYear}].[dbo].[RegDst_Inst].[DstCode]=[${dbYear}].[dbo].[ZONES].[CODE_ZONE]
            WHERE [CODE_TYPE_ZONE]=2 AND [RegCode]=${regionID}`
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
          "LevelTwo": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_TYPE_TVET_INSTITUTION], [DESCRIPTION_TYPE_TVET_INSTITUTION]
            FROM [db2018_Ghana].[dbo].[TYPE_TVET_INSTITUTION]`
          )).map(async (level: any) => {
            return {
              "Level": level.DESCRIPTION_TYPE_TVET_INSTITUTION,
              "Proficiency": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Certificate": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Intermediate": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Advance": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [RegCode]=${regionID} AND ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
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
          "LevelTwo": await Promise.all((await this.mssqldbDataSource.execute(
            `SELECT DISTINCT [CODE_TYPE_TVET_INSTITUTION], [DESCRIPTION_TYPE_TVET_INSTITUTION]
            FROM [db2018_Ghana].[dbo].[TYPE_TVET_INSTITUTION]`
          )).map(async (level: any) => {
            return {
              "Level": level.DESCRIPTION_TYPE_TVET_INSTITUTION,
              "Proficiency": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=33 OR [ORDER_TYPE_GRADE]=34) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Certificate": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=38 OR [ORDER_TYPE_GRADE]=39 OR [ORDER_TYPE_GRADE]=40 OR [ORDER_TYPE_GRADE]=41 OR [ORDER_TYPE_GRADE]=42) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Intermediate": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND [ORDER_TYPE_GRADE]=255 AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
              },
              "Advance": {
                "Male": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([MALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Female": (await this.mssqldbDataSource.execute(
                  `SELECT ISNULL(SUM([FEMALE]),0) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total,
                "Value": (await this.mssqldbDataSource.execute(
                  `SELECT (ISNULL(SUM([MALE]),0) + ISNULL(SUM([FEMALE]),0)) as Total
                  FROM [${dbYear}].[dbo].[RegDst_Inst]
                  INNER JOIN [${dbYear}].[dbo].[ENROLMENT_TVET]
                  ON [${dbYear}].[dbo].[RegDst_Inst].[CODE_INSTITUTION]=[${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[INSTITUTION_INFORMATION]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_INSTITUTION]=[${dbYear}].[dbo].[INSTITUTION_INFORMATION].[CODE_INSTITUTION]
                  INNER JOIN [${dbYear}].[dbo].[TYPE_GRADE]
                  ON [${dbYear}].[dbo].[ENROLMENT_TVET].[CODE_TYPE_GRADE]=[${dbYear}].[dbo].[TYPE_GRADE].[CODE_TYPE_GRADE]
                  WHERE [DstCode]=${districtID} AND ([ORDER_TYPE_GRADE]=43 OR [ORDER_TYPE_GRADE]=44) AND [CODE_TYPE_TVET_INSTITUTION]=${level.CODE_TYPE_TVET_INSTITUTION}`
                ))[0].Total
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
          }))).filter((a: any) => a.Value > 0).sort((a: any, b: any) => a.Value - b.Value).slice(0, 5)
        }
      }
    } catch (error) {
      throw new HttpErrors.ExpectationFailed(error.message);
    }
  }
}
