// Uncomment these imports to begin using these cool features!
import {service} from '@loopback/core';
import {get, HttpErrors, param} from '@loopback/rest';
import {TvetClassroomStructureStateService, TvetEnrolmentSchoolLevelService, TvetInstitutionsService, TvetSchoolExaminingBoardService, TvetSchoolFacilitiesService, TvetSchoolInfrastructureService, TvetSchoolKindService, TvetStudentAgeGroupingService, TvetTeacherStatusService, TvetTechnicalTeacherStatusService} from '../services';

const INDICATOR_IDS = {
  TVET_INSTITUTIONS: 31,
  TVET_ENROLMENT_SCHOOL_LEVEL: 27,
  TVET_STUDENT_AGE_GROUPING: 28,
  TVET_TEACHER_STATUS: 29,
  TVET_TECHNICAL_TEACHER_STATUS: 30,
  TVET_SCHOOL_INFRASTRUCTURE: 32,
  TVET_CLASSROOM_STRUCTURE_STATE: 33,
  TVET_SCHOOL_EXAMINING_BODY: 35,
  TVET_SCHOOL_FACILITIES: 34,
  TVET_SCHOOL_KIND: 36,
}

export class IndicatorController {
  constructor(
    @service(TvetInstitutionsService) private tvetInstitutionsService: TvetInstitutionsService,
    @service(TvetEnrolmentSchoolLevelService) private tvetEnrolmentSchoolLevelService: TvetEnrolmentSchoolLevelService,
    @service(TvetStudentAgeGroupingService) private tvetStudentAgeGroupingService: TvetStudentAgeGroupingService,
    @service(TvetTeacherStatusService) private tvetTeacherStatusService: TvetTeacherStatusService,
    @service(TvetTechnicalTeacherStatusService) private tvetTechnicalTeacherStatusService: TvetTechnicalTeacherStatusService,
    @service(TvetSchoolInfrastructureService) private tvetSchoolInfrastructureService: TvetSchoolInfrastructureService,
    @service(TvetClassroomStructureStateService) private tvetClassroomStructureStateService: TvetClassroomStructureStateService,
    @service(TvetSchoolExaminingBoardService) private tvetSchoolExaminingBoardService: TvetSchoolExaminingBoardService,
    @service(TvetSchoolFacilitiesService) private tvetSchoolFacilitiesService: TvetSchoolFacilitiesService,
    @service(TvetSchoolKindService) private tvetSchoolKindService: TvetSchoolKindService,
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
    } else if (indicatorID === INDICATOR_IDS.TVET_TEACHER_STATUS) {
      return this.tvetTeacherStatusService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_TECHNICAL_TEACHER_STATUS) {
      return this.tvetTechnicalTeacherStatusService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_INFRASTRUCTURE) {
      return this.tvetSchoolInfrastructureService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_CLASSROOM_STRUCTURE_STATE) {
      return this.tvetClassroomStructureStateService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_EXAMINING_BODY) {
      return this.tvetSchoolExaminingBoardService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_FACILITIES) {
      return this.tvetSchoolFacilitiesService.getNational(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_KIND) {
      return this.tvetSchoolKindService.getNational(year);
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
    } else if (indicatorID === INDICATOR_IDS.TVET_TEACHER_STATUS) {
      return this.tvetTeacherStatusService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_TECHNICAL_TEACHER_STATUS) {
      return this.tvetTechnicalTeacherStatusService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_INFRASTRUCTURE) {
      return this.tvetSchoolInfrastructureService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_CLASSROOM_STRUCTURE_STATE) {
      return this.tvetClassroomStructureStateService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_EXAMINING_BODY) {
      return this.tvetSchoolExaminingBoardService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_FACILITIES) {
      return this.tvetSchoolFacilitiesService.getRegional(year, regionID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_KIND) {
      return this.tvetSchoolKindService.getRegional(year, regionID);
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
    } else if (indicatorID === INDICATOR_IDS.TVET_TEACHER_STATUS) {
      return this.tvetTeacherStatusService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_TECHNICAL_TEACHER_STATUS) {
      return this.tvetTechnicalTeacherStatusService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_INFRASTRUCTURE) {
      return this.tvetSchoolInfrastructureService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_CLASSROOM_STRUCTURE_STATE) {
      return this.tvetClassroomStructureStateService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_EXAMINING_BODY) {
      return this.tvetSchoolExaminingBoardService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_FACILITIES) {
      return this.tvetSchoolFacilitiesService.getDistrict(year, districtID);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_KIND) {
      return this.tvetSchoolKindService.getDistrict(year, districtID);
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
    } else if (indicatorID === INDICATOR_IDS.TVET_TEACHER_STATUS) {
      return this.tvetTeacherStatusService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_TECHNICAL_TEACHER_STATUS) {
      return this.tvetTechnicalTeacherStatusService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_INFRASTRUCTURE) {
      return this.tvetSchoolInfrastructureService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_CLASSROOM_STRUCTURE_STATE) {
      return this.tvetClassroomStructureStateService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_EXAMINING_BODY) {
      return this.tvetSchoolExaminingBoardService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_FACILITIES) {
      return this.tvetSchoolFacilitiesService.getTopBottom5(year);
    } else if (indicatorID === INDICATOR_IDS.TVET_SCHOOL_KIND) {
      return this.tvetSchoolKindService.getTopBottom5(year);
    } else {
      throw new HttpErrors.NotFound('Top Bottom 5 Not Implemented');
    }
  }
}
