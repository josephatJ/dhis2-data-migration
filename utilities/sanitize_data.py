
# Josephat Mwakyusa, August 10, 2022
from datetime import datetime
import json
import os

from utilities.generate_ids import get_random_string

async def formulate_data_elements_from_department(department, ids, idsawt,idsspt,timeids, presetids, hrhids, suppyids, indids):
    data_elements = []
    count = 0
    for cadre in department['cadres']:
        hmis_data_element = {}
        hmis_data_element['id'] = get_random_string(11)
        hmis_data_element['name'] = 'HMIS_# of ' + department['name'] + "_" + cadre['code']
        hmis_data_element['displayShortName'] = ('HMIS_# of ' + department['name'] + "_" + cadre['code'])[0:50]
        hmis_data_element['shortName'] = ('HMIS_# of ' + department['name'] + "_" + cadre['code'])[0:50]
        hmis_data_element['displayName'] =  'HMIS_# of ' + department['name'] + "_" + cadre['code']
        hmis_data_element['dataSetElements'] = []
        hmis_data_element['valueType'] = 'NUMBER'
        hmis_data_element['aggregationType'] = 'SUM'
        hmis_data_element['domainType'] = 'AGGREGATE'
        hmis_data_element['dimensionItem'] = ''
        hmis_data_element['zeroIsSignificant'] = True
        hmis_data_element['code'] = ''

        data_elements.append(hmis_data_element)

        awt_elems = generate_AWTDE(cadre, department,idsawt)
        spt_activities_elems = generate_support_activities_DE(cadre, department,idsspt)
        ts_elems = generate_time_standard_DE([hmis_data_element], cadre, department, timeids)
        preset_elems = generate_preset_DE([hmis_data_element], cadre, department, presetids)
        hrh_elems = generate_HRHDE(cadre,department, hrhids)
        supply_elems =  generate_supply_constraints_DE(cadre,department,"")
        count = count + 1
        data_elements = [*data_elements,  *awt_elems]
        data_elements = [*data_elements,  *spt_activities_elems]
        data_elements = [*data_elements,  *ts_elems]
        data_elements = [*data_elements,  *preset_elems]
        data_elements = [*data_elements,  *hrh_elems]
        data_elements = [*data_elements,  *supply_elems]

        indicators = get_staff_requirement_indicator(hmis_data_element,data_elements, cadre, department, "")
        count = count + 1
    
    # print(json.dumps(data_elements))
    return {"dataElements": data_elements, "indicators": indicators}

def generate_AWTDE(cadre, department, ids):
    actitivities_types = [
        '_Working hours per day',
        '_Working days per week',
        '_Training Days per Year',
        '_Special No Notice Leave',
        '_Sick leave',
        '_Public holidays',
        '_Annual leave',
    ]
    count = 0
    awt_elems = []
    for actitivities_type in actitivities_types:
        elem = {}
        elem['id'] = get_random_string(11)
        elem['name'] = department['code'] + "_" + cadre['code'] + actitivities_type
        elem['displayName'] = department['code'] + "_" + cadre['code'] + actitivities_type
        elem['displayFormName'] = department['code'] + "_" + cadre['code'] + actitivities_type
        elem['displayShortName'] = (department['code'] + "_" + cadre['code'] + actitivities_type)[0:50]
        elem['shortName'] = (department['code'] + "_" + cadre['code'] + actitivities_type)[0:50]
        elem['dataSetElements'] = []
        elem['valueType'] = 'NUMBER'
        elem['aggregationType'] = 'SUM'
        elem['domainType'] = 'AGGREGATE'
        elem['dimensionItem'] = ''
        elem['zeroIsSignificant'] = True
        elem['code'] = ''
        awt_elems.append(elem)
        count = count + 1
    return awt_elems


def generate_support_activities_DE(cadre, department, ids):
    actitivites_types = ['Administration_', 'Outreach_']
    count = 0
    elems = []
    for activity_type in actitivites_types:
        elem = {}
        elem['id'] = get_random_string(11)
        elem['name'] = activity_type + department['code'] + cadre['code']
        elem['displayName'] = activity_type + department['code'] + cadre['code']
        elem['displayFormName'] = activity_type + department['code'] + cadre['code']
        elem['displayShortName'] = (activity_type + department['code'] + cadre['code'])[0:50]
        elem['shortName'] = (activity_type + department['code'] + cadre['code'])[0:50]
        elem['dataSetElements'] = []
        elem['valueType'] = 'NUMBER'
        elem['aggregationType'] = 'SUM'
        elem['domainType'] = 'AGGREGATE'
        elem['dimensionItem'] = ''
        elem['zeroIsSignificant'] = True
        elem['code'] = ''
        elems.append(elem)
        count = count + 1
    return elems
    
def generate_time_standard_DE(hmis_elems,cadre,department,ids):
    elems = []
    count = 0
    for hmis_elem in hmis_elems:
        elem = {}
        elem['id'] = get_random_string(11)
        elem['name'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_"+ department['code']
        elem['displayName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayFormName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayShortName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_" + department['code']
        elem['shortName'] = ((hmis_elem['shortName'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_" + department['code'])[3:100])[0:50]
        elem['dataSetElements'] = []
        elem['valueType'] = 'NUMBER'
        elem['aggregationType'] = 'SUM'
        elem['domainType'] = 'AGGREGATE'
        elem['zeroIsSignificant'] = True
        elem['dimensionItem'] = ''
        elem['code'] = ''
        count = count + 1
        elems.append(elem)
    return elems

def generate_preset_DE(hmis_elems,cadre,department,ids):
    elems = []
    count = 0
    for hmis_elem in hmis_elems:
        elem = {}
        elem['id'] = get_random_string(11)
        elem['name'] = hmis_elem['name'].replace('HMIS_# of ', '%') + '_' + cadre['name'] + "_" + department['code']
        elem['displayName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayFormName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayShortName'] ='%' + ((hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_"+ department['code'])[4:100])[0:49]
        elem['shortName'] = '%' +(('%' +hmis_elem['shortName'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_" + department['code'])[4:100])[0:50]
        elem['dataSetElements'] = []
        elem['valueType'] = 'NUMBER'
        elem['aggregationType'] = 'SUM'
        elem['domainType'] = 'AGGREGATE'
        elem['dimensionItem'] = ''
        elem['zeroIsSignificant'] = True
        elem['code'] = ''
        elems.append(elem)
        count = count + 1
    return elems

def generate_HRHDE(cadre,department,ids):
    elems = []
    count = 0
    extensions = [{ "extension": ".", "valueType": 'NUMBER' },{ "extension": ' staff needed', "valueType": 'NUMBER' }]
    for extension in extensions:
        elem = {}
        elem['id'] = get_random_string(11)
        elem['name'] = department['name'] + cadre['name'] + extension['extension']
        elem['displayName'] = department['name'] + cadre['name'] + extension['extension']
        elem['displayFormName'] = department['name'] + cadre['name'] + extension['extension']
        elem['displayShortName'] = (department['name'] + cadre['name'] + extension['extension'])[0:50]
        elem['shortName'] = (department['code'] + cadre['code'] + extension['extension'])[0:50]
        elem['valueType'] = extension['valueType']
        elem['aggregationType'] = 'LAST'
        elem['domainType'] = 'AGGREGATE'
        elem['zeroIsSignificant'] = True
        elem['dataSetElements'] = []
        elem['dimensionItem'] = ''
        elem['code'] = ''
        elem['translations'] = []
        elem['userGroupAccesses'] = []
        elem['dataElementGroups'] = []
        elem['attributeValues'] = []
        elem['userAccesses'] = []
        elem['legendSets'] = []
        elem['aggregationLevels'] = []
        elems.append(elem)
        count = count + 1
    return elems

def generate_supply_constraints_DE(cadre,department,id_ref):
    elems = []
    elem = {
            "id": get_random_string(11),
            "name": 'Supply Constraint ' + cadre['name'],
            "displayName": 'Supply Constraint ' + cadre['name'],
            "formName": cadre['name'],
            "displayFormName": cadre['name'],
            "displayShortName": ('Supply Const ' + cadre['code'])[0:50],
            "shortName": ('Supply Const ' + cadre['code'])[0:50],
            "valueType": 'INTEGER_ZERO_OR_POSITIVE',
            "aggregationType": 'SUM',
            "domainType": 'AGGREGATE',
            "zeroIsSignificant": True,
            "dataSetElements": [],
            "dimensionItem": '',
            "code": ''
        }
    elems.append(elem)
    return elems

def get_staff_requirement_indicator(hmis_elem, data_elements_by_cadre, cadre, department, ind_id):
    aggregate_data_elements = data_elements_by_cadre
    allocation_dx_name = '%' + hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + '_' + department['code']
    timestandard_dx_name ='' + hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']

    # get_dx_uid
    noOfHours = '(52-((#{' + get_dx_uid((department['code'] + "_" + cadre['code'] + '_Annual leave'),aggregate_data_elements) +'}+' + '#{'
    noOfHours += get_dx_uid((department['code'] + "_" + cadre["code"] + '_Public holidays'), aggregate_data_elements)
    noOfHours += '}+'
    noOfHours += '#{'
    noOfHours += get_dx_uid((department['code'] + "_" + cadre['code'] + '_Sick leave'),aggregate_data_elements)
    noOfHours += '}+'
    noOfHours += '#{' + get_dx_uid((department['code'] + "_" + cadre['code'] + '_Special No Notice Leave'),aggregate_data_elements)
    noOfHours += '}+'
    noOfHours += '#{'
    noOfHours += get_dx_uid((department['code'] + "_" + cadre["code"] + '_Training Days per Year'),aggregate_data_elements)
    noOfHours += '})/'
    noOfHours += '#{'
    noOfHours += get_dx_uid((department['code'] + "_" + cadre['code'] + '_Working days per week'),aggregate_data_elements)
    noOfHours += '}))*'
    noOfHours += '#{'
    noOfHours += get_dx_uid((department['code'] + "_" + cadre['code'] + '_Working days per week'),aggregate_data_elements)
    noOfHours += '}*'
    noOfHours += '#{'
    noOfHours += get_dx_uid((department['code'] + "_" + cadre['code'] + '_Working hours per day'),aggregate_data_elements)
    noOfHours += '}'

    patient_break_down = '#{' +  hmis_elem['id'] + "}*(#{" + get_dx_uid(allocation_dx_name,aggregate_data_elements) + "}/100)"
    tspc = "((" + patient_break_down + ")*#{" +  get_dx_uid(timestandard_dx_name,aggregate_data_elements) + "})/60"

    staffReq = "(" + tspc + ")/(" + noOfHours + ")"
    casDxname = "Administration_" + department['code'] + cadre['code']
    casDxId = get_dx_uid(casDxname, aggregate_data_elements)
    cis_dx_name = "Outreach_" + department['code'] + cadre['code']
    cisDxId = get_dx_uid(cis_dx_name, aggregate_data_elements)
    numerator = "((" + staffReq + ")*#{" + casDxId + "})+#{" + cisDxId + "}"

    inds = [
        {
            "id": get_random_string(11),
            "name": 'Staff Requirement ' + department['code'] + "_" + cadre['code'],
            "shortName": ('Staff Requirement ' + department['code'] + "_" + cadre['code'])[0:50],
            "denominatorDescription": 'Denominator is 1',
            "numeratorDescription": 'Staff Requirement ' + department['code'] + "_" + cadre['code'],
            "numerator": numerator,
            "denominator": '1',
            "annualized": False,
            "decimals": 2,
            "indicatorType": {
                "id": 'fJAlH7FSAeG',
            }
        }
    ]
    return inds

def get_dx_uid(elem_name, aggregate_data_elements):
    filtered_elems = []
    for elem in aggregate_data_elements:
        if elem['name'] == elem_name:
            filtered_elems.append(elem)
    if len(filtered_elems) > 0:
        return str(filtered_elems[0]['id'])
    else:
        return ''

def remove_error_issues(errorDetails):
    print("errorDetails", errorDetails)
    return []

def formulate_current_dpt_configs(department, selected_cadres, metadata):
    group = ""
    if "group" in department:
        group = department['group']
    department_config = {
            "id": department['code'],
            "name": department['name'],
            "group": group,
            "cadres": formulate_cadres_object(selected_cadres, metadata, department)
        }
    return department_config

def get_hrh_dataelement(cadre, dataelements,department):
    hrh_elem = ""
    for dataelement in dataelements:
        if dataelement['name'] == department['name'] + cadre['name'] + ".":
            hrh_elem =  dataelement['id']
    return hrh_elem

def get_need_indicator(cadre, indicators,department):
    need_elem = ""
    for indicator in indicators:
        if (department['code'] + "_" + cadre['code']) in indicator['name']:
            need_elem =  indicator['id']
    return need_elem

def formulate_cadres_object(cadres, metadata, department):
    cadre_items = []
    for cadre in cadres:
        hrh_elem_id = get_hrh_dataelement(cadre, metadata['dataElements'], department)
        need_ind_id = get_need_indicator(cadre, metadata['indicators'], department)
        cadre_type =''
        cadre_group = ''
        if 'type' in cadre:
            cadre_type = cadre['type']
        if 'group' in cadre:
            cadre_type = cadre['group']
        cadre_item = {
                "id": cadre['code'],
                "name": cadre['name'],
                "type": cadre_type,
                "group": cadre_group,
                "dataSource": [
                    {
                        "id": '',
                        "label": 'Organization unit',
                        "sortOrder": 0,
                        "calculated": False,
                    },
                    {
                        "id": hrh_elem_id,
                        "type": 'indicators',
                        "isHRH": True,
                        "label": 'Existing Staff',
                        "sortOrder": 1,
                        "calculated": True,
                    },
                    {
                        "id": need_ind_id,
                        "type": 'indicators',
                        "label": 'Calculated staff requirement',
                        "sortOrder": 2,
                        "calculated": True,
                    },
                    {
                        "id": '',
                        "type": 'indicators',
                        "label": "WISN Ratio " + cadre['code'],
                        "sortOrder": 3,
                        "calculated": False,
                    },
                ]
            }
        cadre_items.append(cadre_item)
    return cadre_items


def is_department_already_on_datastore(departments, current_department):
    exist = False
    for dpt in departments:
        if 'id' in dpt:
            if dpt['id'] == current_department['code']:
                exist = True
    print("########################################################################")
    print("########################################################################")
    print(exist)
    print("########################################################################")
    print("########################################################################")
    return exist

def remove_duplicate_cadres(cadres):
    check = {}
    check['testing'] = 'testing'
    new_cadres = []
    for cadre in cadres:
        cadre_id = cadre['id']
        if cadre_id not in check:
            check[cadre['id']] = cadre['id']
            new_cadres.append(cadre)
    return new_cadres

def merge_departments(all_dpts, new_dpt):
    new_all_dpts = []
    for dpt in all_dpts:
        if 'id' in dpt:
            if dpt['id'] == new_dpt['id']:
                new_all_dpts.append(new_dpt)
            else:
                new_all_dpts.append(dpt)
        else:
            new_all_dpts.append(dpt)
    return new_all_dpts


def clean_metadata_from_errors(response, metadata):
    elems_to_omit = {}
    inds_to_omit = {}
    elems_has_error = False
    inds_has_error = False
    for typeReport in response['typeReports']:
        if typeReport['klass'] == "org.hisp.dhis.dataelement.DataElement":
            if 'objectReports' in typeReport:
                for obj_report in typeReport['objectReports']:
                    elems_has_error = True
                    for error_report in obj_report['errorReports']:
                        elem_id = error_report['message'].split("[")[1].split("]")[0]
                        elems_to_omit[elem_id] = error_report['errorProperties'][3]
        if typeReport['klass'] == "org.hisp.dhis.indicator.Indicator":
            if 'objectReports' in typeReport:
                for obj_report in typeReport['objectReports']:
                    inds_has_error = True
                    for error_report in obj_report['errorReports']:
                        ind_id = error_report['message'].split("[")[1].split("]")[0]
                        inds_to_omit[ind_id] = error_report['errorProperties'][3]
    path =  os.getcwd() + "/metadata/exists.json"
    with open(path, "w") as jsonfile:
        jsonfile.write(json.dumps(elems_to_omit))
    new_dataelements = []
    new_indicators = []
    if elems_has_error == True:
        for elem in metadata['dataElements']:
            if elem['id'] not in elems_to_omit:
                new_dataelements.append(elem)
            else:
                new_elem = elem
                # TODO: Remember to update indicator expression accordingly
                new_elem['id'] = elems_to_omit[elem['id']]
                indicators_updated = []
                for indicator in metadata['indicators']:
                    new_ind = indicator
                    new_ind['numerator'] = indicator['numerator'].replace(elem['id'],new_elem['id'] )
                    indicators_updated.append(new_ind)
                metadata['indicators'] = indicators_updated
                new_dataelements.append(new_elem)
    
    if inds_has_error == True:
        for ind in metadata['indicators']:
            if ind['id'] not in inds_to_omit:
                new_indicators.append(ind)
            else:
                new_ind = ind
                new_ind['id'] = inds_to_omit[elem['id']]
                new_indicators.append(new_ind)
    
    return {
        "dataElements": new_dataelements,
        "indicators": new_indicators
    }


def formulate_datastore_payload(stored_dpts_rows):
    data = {}
    for row in stored_dpts_rows:
        value = json.loads(row[2]['value'])
        if 'name' in value and 'softDeleted' not in value:
            data[value['name']] = value
    return data

def format_cadre_for_datastore(data_row):
    current_date = datetime.now().timestamp()
    cadre = {
        "cadreBasicInfo": {
                "id": get_random_string(11),
                "name": data_row[2],
                "code": data_row[3],
                "group": data_row[4],
                "type": data_row[5],
                "created": current_date,
                "lastUpdated": current_date,
                "createdBy": "lkpMY7Ys94v"
            },
        "monthlySalary":  data_row[6]
    }
    return cadre

def format_cadre_presets(cadre_details):
    cadre_preset = {
                    "id": cadre_details['cadreBasicInfo']['id']+"_2022",
                    "data": {
                        "costAndSupply": {
                        "monthlySalary": cadre_details['monthlySalary'],
                        "supply": "0",
                        "porlagSupply": ""
                        },
                        "wisnParameters": {
                        "dispensary": {
                            "mainActivity": {
                            "opd": { "opd_min_client": "", "opd_allocation": "" },
                            "ipd": { "ipd_min_client": "", "ipd_allocation": "" },
                            "labour-and-delivery": { "ld_min_client": "", "ld_allocation": "" },
                            "c_section": {
                                "c_section_min_client": "",
                                "c_section_allocation": ""
                            },
                            "anc": { "anc_min_client": "", "anc_allocation": "" },
                            "postnatal": {
                                "postnatal_min_client": "",
                                "postnatal_allocation": ""
                            },
                            "child-health": {
                                "child_health_min_client": "",
                                "child_health_allocation": ""
                            },
                            "nacp": { "nacp_min_client": "", "nacp_allocation": "" }
                            },
                            "supportActivity": {
                            "administration_hours_per_week": "",
                            "outreach_hours_per_week": ""
                            },
                            "workingTime": {
                            "working_days": "",
                            "working_hours": "",
                            "annual_leave": "",
                            "public_holidays": "",
                            "sick_leave": "",
                            "special_no_notice_leave": "",
                            "training_days": ""
                            }
                        },
                        "health-center": {
                            "mainActivity": {
                            "opd": { "opd_min_client": "", "opd_allocation": "" },
                            "ipd": { "ipd_min_client": "", "ipd_allocation": "" },
                            "labour-and-delivery": { "ld_min_client": "", "ld_allocation": "" },
                            "c_section": {
                                "c_section_min_client": "",
                                "c_section_allocation": ""
                            },
                            "anc": { "anc_min_client": "", "anc_allocation": "" },
                            "postnatal": {
                                "postnatal_min_client": "",
                                "postnatal_allocation": ""
                            },
                            "child-health": {
                                "child_health_min_client": "",
                                "child_health_allocation": ""
                            },
                            "nacp": { "nacp_min_client": "", "nacp_allocation": "" }
                            },
                            "supportActivity": {
                            "administration_hours_per_week": "",
                            "outreach_hours_per_week": ""
                            },
                            "workingTime": {
                            "working_days": "",
                            "working_hours": "",
                            "annual_leave": "",
                            "public_holidays": "",
                            "sick_leave": "",
                            "special_no_notice_leave": "",
                            "training_days": ""
                            }
                        },
                        "health-center-bemonc": {
                            "mainActivity": {
                            "opd": { "opd_min_client": "", "opd_allocation": "" },
                            "ipd": { "ipd_min_client": "", "ipd_allocation": "" },
                            "labour-and-delivery": { "ld_min_client": "", "ld_allocation": "" },
                            "c_section": {
                                "c_section_min_client": "",
                                "c_section_allocation": ""
                            },
                            "anc": { "anc_min_client": "", "anc_allocation": "" },
                            "postnatal": {
                                "postnatal_min_client": "",
                                "postnatal_allocation": ""
                            },
                            "child-health": {
                                "child_health_min_client": "",
                                "child_health_allocation": ""
                            },
                            "nacp": { "nacp_min_client": "", "nacp_allocation": "" }
                            },
                            "supportActivity": {
                            "administration_hours_per_week": "",
                            "outreach_hours_per_week": ""
                            },
                            "workingTime": {
                            "working_days": "",
                            "working_hours": "",
                            "annual_leave": "",
                            "public_holidays": "",
                            "sick_leave": "",
                            "special_no_notice_leave": "",
                            "training_days": ""
                            }
                        },
                        "hospital": {
                            "mainActivity": {
                            "opd": { "opd_min_client": "", "opd_allocation": "" },
                            "ipd": { "ipd_min_client": "", "ipd_allocation": "" },
                            "labour-and-delivery": { "ld_min_client": "", "ld_allocation": "" },
                            "c_section": {
                                "c_section_min_client": "",
                                "c_section_allocation": ""
                            },
                            "anc": { "anc_min_client": "", "anc_allocation": "" },
                            "postnatal": {
                                "postnatal_min_client": "",
                                "postnatal_allocation": ""
                            },
                            "child-health": {
                                "child_health_min_client": "",
                                "child_health_allocation": ""
                            },
                            "nacp": { "nacp_min_client": "", "nacp_allocation": "" }
                            },
                            "supportActivity": {
                            "administration_hours_per_week": "",
                            "outreach_hours_per_week": ""
                            },
                            "workingTime": {
                            "working_days": "",
                            "working_hours": "",
                            "annual_leave": "",
                            "public_holidays": "",
                            "sick_leave": "",
                            "special_no_notice_leave": "",
                            "training_days": ""
                            }
                        }
                        }
                    }
                }
    return cadre_preset