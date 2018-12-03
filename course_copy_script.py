import maya
import requests
import json
import pandas as pd


api_key = ''
header = {'Authorization': 'Bearer ' + api_key}

base_url = 'https://domainnamehere.instructure.com/api/v1'
# Pull courses from previous day
pulled_courses_csv = ''
terms_to_filter = ['sis_term_id', 'sis_term_id', 'sis_term_id']
accounts_to_filter = ['sis_id_for_account', 'sis_id_for_account', 'sis_id_for_account']

courses_csv_to_copy = 'copied_courses.csv'

course_canvas_id_to_copy = ''
courses_copied_final = 'courses_copied.csv'


# Using the courses endpoint to update settings

# download the course files

def main():
    yesterdays__imports = get_yesterdays_courses()
    course_download_urls = get_course_download_urls(yesterdays__imports)
    get_course_data_new(course_download_urls)
    data_to_get_activity = read_to_pandas_and_filter_accounts_and_terms()
    build_activity_report(data_to_get_activity)
    df_with_activity = build_activity_report(data_to_get_activity)
    courses_to_copy = filter_activity(df_with_activity)
    new_course_copy_df = courses_to_copy.copy()
    new_course_copy_df['migration_status'] = new_course_copy_df.course_id.apply(course_copy)
    new_course_copy_df.to_csv(courses_copied_final)





# minus a day
def get_yesterdays_courses():
    yesterday = maya.when('yesterday')
    date = yesterday.iso8601()

    sis_import_url = '{baseurl}/accounts/self/sis_imports'.format(baseurl=base_url)
    all_params = {'created_since': date}
    r = requests.get(sis_import_url, headers=header, params=all_params)
    sis_imports = json.loads(r.text)
    while 'next' in r.links:
        r = requests.get(url=r.links['next']['url'], headers=header)
        sis_imports = json.loads(r.text)

    return sis_imports


def check_account_to_filter(account_row_value):
    if account_row_value in accounts_to_filter:
        return True
    else:
        return False


def check_term_to_filter(term_row_value):
    if term_row_value in accounts_to_filter:
        return True
    else:
        return False


def get_page_activity(course_id):
    activity_url = '{baseurl}/courses/{courseid}/pages'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)


def get_assignment_activity(course_id):
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/assignments'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)


def get_quiz_activity(course_id):
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/quizzes'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)


def get_module_activity(course_id):
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/modules'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)


def get_discussion_activity(course_id):
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/discussion_topics'.format(baseurl=base_url,
                                                                                         courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)


def get_course_download_urls(sis_imports):
    all_course_urls = []
    for item in sis_imports['sis_imports']:
        for data in item['csv_attachments']:
            all_course_urls.append(data['url'])
    return all_course_urls


def get_course_data_new(url_list):
    for item in url_list:
        r = requests.get(item, headers=header)
        if r.status_code == 200:
            open(pulled_courses_csv, 'wb').write(r.content)
        else:
            print('failed with status code: ' + r.status_code)


def read_to_pandas_and_filter_accounts_and_terms():
    previous_day = pd.read_csv(pulled_courses_csv)

    previous_day['remove_account'] = previous_day.account_id.apply(check_account_to_filter)
    previous_day['remove_term'] = previous_day.account_id.apply(check_term_to_filter)
    data_to_get_activity = previous_day.loc[(previous_day['remove_account'] == False) & (previous_day['remove_term'] == False)]
    return data_to_get_activity


def build_activity_report(data_to_get_all_activity):
    data_to_get_all_activity['page_activity'] = data_to_get_all_activity.course_id.apply(get_page_activity)
    data_to_get_all_activity['assignment_activity'] = data_to_get_all_activity.course_id.apply(get_assignment_activity)
    data_to_get_all_activity['quiz_activity'] = data_to_get_all_activity.course_id.apply(get_quiz_activity)
    data_to_get_all_activity['module_activity'] = data_to_get_all_activity.course_id.apply(get_module_activity)
    data_to_get_all_activity['discussion_activity'] = data_to_get_all_activity.course_id.apply(get_discussion_activity)
    return data_to_get_all_activity


def filter_activity(report_with_activity):

    filtered_activity_dataframe = report_with_activity.loc[(report_with_activity['page_activity'] <= 0) & (report_with_activity['assignment_activity'] <= 0) & (report_with_activity['quiz_activity'] <= 0) & (report_with_activity['module_activity'] <= 0) & (report_with_activity['discussion_activity'] <= 0)]

    return filtered_activity_dataframe




def course_copy(course_id):
    course_copy_url = '{baseurl}/courses/sis_course_id:{courseid}/content_migrations?migration_type=course_copy_importer&settings[source_course_id]={sis_course}'.format(baseurl=base_url, sis_course=course_canvas_id_to_copy, courseid=course_id)
    r = requests.post(course_copy_url, headers=header)
    if r.ok:
        return r.status_code
    else:
        return 'failed with status code: {}'.format(r.status_code)




if __name__ == '__main__':
    main()
