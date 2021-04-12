// Uncomment these imports to begin using these cool features!
import {service} from '@loopback/core';
import {get, HttpErrors, param} from '@loopback/rest';
import {TvetEnrolmentSchoolLevelService, TvetInstitutionsService, TvetStudentAgeGroupingService} from '../services';

const INDICATOR_IDS = {
  TVET_INSTITUTIONS: 31,
  TVET_ENROLMENT_SCHOOL_LEVEL: 27,
  TVET_STUDENT_AGE_GROUPING: 28,
}

export class IndicatorController {
  constructor(
    @service(TvetInstitutionsService)
    private tvetInstitutionsService: TvetInstitutionsService,
    @service(TvetEnrolmentSchoolLevelService)
    private tvetEnrolmentSchoolLevelService: TvetEnrolmentSchoolLevelService,
    @service(TvetStudentAgeGroupingService)
    private tvetStudentAgeGroupingService: TvetStudentAgeGroupingService
  ) { }

  @get('/loopback/get_indicator_national')
  async getIndicatorNational(
    @param.query.number('year', {required: true}) year: number,
    @param.query.number('indicatorID', {required: true}) indicatorID: number,
  ) {
    if (indicatorID === INDICATOR_IDS.TVET_INSTITUTIONS) {
      return this.tvetInstitutionsService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_ENROLMENT_SCHOOL_LEVEL) {
      return this.tvetEnrolmentSchoolLevelService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_STUDENT_AGE_GROUPING) {
      return this.tvetStudentAgeGroupingService.getNational(year);
    } else {
      throw new HttpErrors.NotFound('National Indicator Not Implemented');
    }
  }

  @get('/loopback/get_indicator_regional')
  async getIndicatorRegional(
    @param.query.number('year', {required: true}) year: number,
    @param.query.number('indicatorID', {required: true}) indicatorID: number,
    @param.query.number('regionID', {required: true}) regionID: number,
  ) {
    if (indicatorID === INDICATOR_IDS.TVET_INSTITUTIONS) {
      return this.tvetInstitutionsService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_ENROLMENT_SCHOOL_LEVEL) {
      return this.tvetEnrolmentSchoolLevelService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_STUDENT_AGE_GROUPING) {
      return this.tvetStudentAgeGroupingService.getRegional(year, regionID);
    } else {
      throw new HttpErrors.NotFound('Regional Indicator Not Implemented');
    }
  }

  @get('/loopback/get_indicator_district')
  async getIndicatorDistrict(
    @param.query.number('year', {required: true}) year: number,
    @param.query.number('indicatorID', {required: true}) indicatorID: number,
    @param.query.number('districtID', {required: true}) districtID: number,
  ) {
    if (indicatorID === INDICATOR_IDS.TVET_INSTITUTIONS) {
      return this.tvetInstitutionsService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_ENROLMENT_SCHOOL_LEVEL) {
      return this.tvetEnrolmentSchoolLevelService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_STUDENT_AGE_GROUPING) {
      return this.tvetStudentAgeGroupingService.getDistrict(year, districtID);
    } else {
      throw new HttpErrors.NotFound('District Indicator Not Implemented');
    }
  }

  @get('/loopback/get_top_bottom_5')
  async getTopBottom5(
    @param.query.number('year', {required: true}) year: number,
    @param.query.number('indicatorID', {required: true}) indicatorID: number,
  ) {
    if (indicatorID === INDICATOR_IDS.TVET_INSTITUTIONS) {
      return this.tvetInstitutionsService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_ENROLMENT_SCHOOL_LEVEL) {
      return this.tvetEnrolmentSchoolLevelService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_STUDENT_AGE_GROUPING) {
      return this.tvetStudentAgeGroupingService.getTopBottom5(year);
    } else {
      throw new HttpErrors.NotFound('Top Bottom 5 Not Implemented');
    }
  }
}
