import os
import re
import requests
from datetime import timedelta

# akvo-webform util
instance_base = 'https://api-auth0.akvo.org/flow/orgs/'
auth_domain = "https://akvofoundation.eu.auth0.com/oauth/token"


def reformat_duration(x):
    return str(timedelta(seconds=x))


def camel_case_split(str):
    matches = re.finditer(
        '.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', str)
    splitted = [m.group(0).lower().strip() for m in matches]
    return " ".join(splitted)


def split_partnership_code(x, index=0):
    if not x:
        return ''
    res = x.split('|')[index]
    if index == 1 or ':' not in res:
        return res
    # get partnership code
    res = res.split(':')[0]
    return res


def split_reporting_period(x, index=0):
    if not x:
        return ''
    return str(x.split('|')[index])


def fill_partnership_code(x, df, target_col):
    # for now we use name column as example
    repeat_index = x.get('repeat no')
    instance = x.get('instance')
    if (repeat_index == 1):
        pass
    find_partnership_code = df[df['instance'] == instance].iloc[0]
    return find_partnership_code.get(target_col)


def find_excel_column_letter(renamed_columns):
    start_index = 1  # it can start either at 0 or at 1
    letter = ''
    column_int = len(renamed_columns)
    while column_int > 25 + start_index:
        letter += chr(65 + int(
            (len(renamed_columns) - start_index) / 26) - 1)
        column_int = column_int - (
            int((len(renamed_columns) - start_index) / 26)) * 26
    letter += chr(65 - start_index + (int(column_int)))
    return letter


def get_headers(token: str):
    login = {
        'client_id': 'S6Pm0WF4LHONRPRKjepPXZoX1muXm1JS',
        'grant_type': 'refresh_token',
        'refresh_token': token,
        'scope': 'openid email'
    }
    req = requests.post(auth_domain, data=login)
    if req.status_code != 200:
        return False
    return {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.akvo.flow.v2+json',
        'Authorization': 'Bearer {}'.format(req.json().get('id_token'))
    }


def get_data(uri, auth):
    req = requests.get(uri, headers=auth).json()
    if req.get('error'):
        print(f"Error 403: {req.get('error')}")
        raise Exception(req.get('error'))
    return req


def fetch_all(url, headers, formInstances=[]):
    data = get_data(url, headers)
    next_url = data.get('nextPageUrl')
    data = data.get('formInstances')
    for d in data:
        formInstances.append(d)
    if next_url:
        fetch_all(next_url, headers, formInstances)
    return formInstances


def handle_list(data, target):
    response = []
    for value in data:
        if value.get("code"):
            response.append("{}:{}".format(value.get("code"),
                                           value.get(target)))
        else:
            response.append(value.get(target))
    return "|".join(response)


def data_handler(data, qType):
    if data:
        if qType in [
                'FREE_TEXT', 'NUMBER', 'BARCODE', 'DATE', 'GEOSHAPE', 'SCAN',
                'CADDISFLY'
        ]:
            return data
        if qType == 'OPTION':
            return handle_list(data, "text")
        if qType == 'CASCADE':
            return handle_list(data, "name")
        if qType == ['PHOTO', 'VIDEO']:
            return data.get('filename')
        if qType == 'VIDEO':
            return data.get('filename')
        if qType == 'GEO':
            return {'lat': data.get('lat'), 'long': data.get('long')}
        if qType == 'SIGNATURE':
            return data.get("name")
    return None


def get_page(instance: str, survey_id: int, form_id: int, token: str):
    headers = get_headers(token)
    instance_uri = '{}{}'.format(instance_base, instance)
    form_instance_url = '{}/form_instances?survey_id={}&form_id={}'.format(
        instance_uri, survey_id, form_id)
    collections = fetch_all(form_instance_url, headers)
    form_definition = get_data('{}/surveys/{}'.format(instance_uri, survey_id),
                               headers)
    form_definition = form_definition.get('forms')
    form_definition = list(
        filter(lambda x: int(x['id']) == form_id,
               form_definition))[0].get('questionGroups')
    results = []
    for col in collections:
        dt = {}
        dt_repeatable = {}
        for c in col:
            if c != 'responses':
                meta = camel_case_split(c)
                dt.update({meta: col[c]})
            else:
                for g in form_definition:
                    for q in g['questions']:
                        try:
                            answers = col[c][g.get('id')]
                            for i, a in enumerate(answers):
                                if i > 0:
                                    dt_repeatable.update({'repeat no': i + 1})
                                    for c in col:
                                        if c != 'responses':
                                            meta = camel_case_split(c)
                                            dt_repeatable.update({
                                                meta: col[c]})
                                    d = data_handler(a.get(q['id']), q['type'])
                                    # n = "{}|{}".format(q['id'], q['name'])
                                    n = q['name'].lower().strip()
                                    dt_repeatable.update({n: d})
                                else:
                                    dt.update({'repeat no': i + 1})
                                    d = data_handler(
                                        answers[0].get(q['id']), q['type'])
                                    # n = "{}|{}".format(q['id'], q['name'])
                                    n = q['name'].lower().strip()
                                    dt.update({n: d})
                        except(TypeError):
                            n = q['name'].lower().strip()
                            dt.update({'repeat no': 1, n: ''})

        results.append(dt)
        if dt_repeatable:
            results.append(dt_repeatable)

    return results


def get_refresh_token():
    data = {
        "client_id": os.environ['the_client_id'],
        "username": os.environ['the_email'],
        "password": os.environ['the_pwd'],
        "grant_type": "password",
        "scope": "offline_access"
    }
    req = requests.post(auth_domain, data=data)
    refresh_token = None
    if req and req.status_code == 200:
        refresh_token = req.json().get('refresh_token')
        f = open('token.txt', 'w')
        f.write(refresh_token)
        f.close()
    return refresh_token
