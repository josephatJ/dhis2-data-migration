
# Josephat Mwakyusa, August 10, 2022
import json

async def formulate_data_elements_from_department(department, ids, idsawt,idsspt,timeids, presetids, hrhids, suppyids, indids):
    data_elements = []
    count = 0
    for cadre in department['cadres']:
        hmis_data_element = {}
        hmis_data_element['id'] = ids[count]
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
        supply_elems =  generate_supply_constraints_DE(cadre,department,suppyids[count])
        ind_id = indids[count]
        count = count + 1
        data_elements = [*data_elements,  *awt_elems]
        data_elements = [*data_elements,  *spt_activities_elems]
        data_elements = [*data_elements,  *ts_elems]
        data_elements = [*data_elements,  *preset_elems]
        data_elements = [*data_elements,  *hrh_elems]
        data_elements = [*data_elements,  *supply_elems]

        indicators = get_staff_requirement_indicator(hmis_data_element,data_elements, cadre, department, ind_id)
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
        elem['id'] = ids[count]
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
        elem['id'] = ids[count]
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
        elem['id'] = ids[count]
        elem['name'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_"+ department['code']
        elem['displayName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayFormName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayShortName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_" + department['code']
        elem['shortName'] = ((hmis_elem['shortName'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_" + department['code'])[3])[0:50]
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
        elem['id'] = ids[count]
        elem['name'] = hmis_elem['name'].replace('HMIS_# of ', '%') + '_' + cadre['name'] + "_" + department['code']
        elem['displayName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayFormName'] = hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['name'] + "_" + department['code']
        elem['displayShortName'] = ((hmis_elem['name'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_"+ department['code'])[4])[0:50]
        elem['shortName'] = (('%' +hmis_elem['shortName'].replace('HMIS_# of ', '') + '_' + cadre['code'] + "_" + department['code'])[4])[0:50]
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
        elem['id'] = ids[count]
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
            "id": id_ref,
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
    wisnNeedNumerator = "((" + staffReq + ")*#{" + casDxId + "})+#{" + cisDxId + "}"

    inds = [
        {
            "id": ind_id,
            "name": 'Staff Requirement ' + department['code'] + "_" + cadre['code'],
            "shortName": ('Staff Requirement ' + department['code'] + "_" + cadre['code'])[0:50],
            "denominatorDescription": 'Denominator is 1',
            "numeratorDescription": 'Staff Requirement ' + department['code'] + "_" + cadre['code'],
            "numerator": wisnNeedNumerator,
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