import maya
import requests
import json
import pandas as pd

#Place API token here
api_key = ''
header = {'Authorization': 'Bearer ' + api_key}
#Replace {domain here} with Canvas Domain
base_url = 'https://{domain here}.instructure.com/api/v1'
# Pull courses from previous day
#set equal to the filename you would like the pulled courses to equal. This will be read in as a pandas dataframe.
pulled_courses_csv = 'canvas_courses.csv'
#Store the sis term id's you do not want included in your copying
terms_to_filter = ['sis_term_id', 'sis_term_id', 'sis_term_id']
accounts_to_filter = ['sis_id_for_account', 'sis_id_for_account', 'sis_id_for_account']
#Store the Canvas ID of the course you would like to copy
course_canvas_id_to_copy = ''
#Store the courses copied with a migration status. This is a report of the courses that made it through the filter and potentially copied.
courses_copied_final = 'courses_copied.csv'

def main():
    yesterdays__imports = get_yesterdays_sisimports()
    course_download_urls = get_course_download_urls(yesterdays__imports)
    get_course_data_new(course_download_urls)
    data_to_get_activity = read_to_pandas_and_filter_accounts_and_terms()
    build_activity_report(data_to_get_activity)
    df_with_activity = build_activity_report(data_to_get_activity)
    courses_to_copy = filter_activity(df_with_activity)
    new_course_copy_df = courses_to_copy.copy()
    new_course_copy_df['migration_status'] = new_course_copy_df.course_id.apply(course_copy)
    new_course_copy_df.to_csv(courses_copied_final)

def get_yesterdays_sisimports():
    """
    Returns yesterdays sis imports
    :return: sis imports from Yesterday
    """
    #Set yesterday to the datetime information
    yesterday = maya.when('yesterday')
    #Convert datetime to ISO
    date = yesterday.iso8601()
    #Get sis imports
    sis_import_url = '{baseurl}/accounts/self/sis_imports'.format(baseurl=base_url)
    all_params = {'created_since': date}
    r = requests.get(sis_import_url, headers=header, params=all_params)
    sis_imports = json.loads(r.text)
    while 'next' in r.links:
        r = requests.get(url=r.links['next']['url'], headers=header)
        sis_imports = json.loads(r.text)
    return sis_imports

def check_account_to_filter(account_row_value):
    """
    Function to check if account should be filtered
    :param account_row_value:
    :return: True if account should be filtered
    """
    if account_row_value in accounts_to_filter:
        return True
    else:
        return False

def check_term_to_filter(term_row_value):
    """
    Function to check if term should be filtered
    :param term_row_value:
    :return: True if term should be filtered
    """
    if term_row_value in accounts_to_filter:
        return True
    else:
        return False

def get_page_activity(course_id):
    """
    Function to check for page activity. A single page creation is considered activity
    :param course_id:
    :return: Page activity count
    """
    activity_url = '{baseurl}/courses/{courseid}/pages'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)

def get_assignment_activity(course_id):
    """
    Function to check for assignment activity. A single assignment creation is considered activity
    :param course_id:
    :return: Assignment activity count
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/assignments'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)

def get_quiz_activity(course_id):
    """
    Function to check for quiz activity. A single quiz creation is considered activity
    :param course_id:
    :return: Quiz activity count
    """
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
    """
    Function to check for discussion activity. A single discussion creation is considered activity
    :param course_id:
    :return: Discussion activity count
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/discussion_topics'.format(baseurl=base_url,
                                                                                courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        return len(course_url)
    else:
        return 'failed with status code: {}'.format(r.status_code)

def get_course_download_urls(sis_imports):
    """
    Function to get the download URLs of courses
    :param sis_imports:
    :return: A list of course URLs from yesterdays imports
    """
    all_course_urls = []
    for item in sis_imports['sis_imports']:
        for data in item['csv_attachments']:
            all_course_urls.append(data['url'])
    return all_course_urls

def get_course_data_new(url_list):
    """
    Function to download course data from yesterdays imports
    :param url_list:
    :return:
    """
    for item in url_list:
        r = requests.get(item, headers=header)
        if r.status_code == 200:
            open(pulled_courses_csv, 'wb').write(r.content)
        else:
            print('failed with status code: ' + r.status_code)

def read_to_pandas_and_filter_accounts_and_terms():
    """
    Function that reads pulled courses and filters accounts and terms
    :return: Dataframe with account information
    """
    previous_day = pd.read_csv(pulled_courses_csv)
    previous_day['remove_account'] = previous_day.account_id.apply(check_account_to_filter)
    previous_day['remove_term'] = previous_day.account_id.apply(check_term_to_filter)
    data_to_get_activity = previous_day.loc[(previous_day['remove_account'] == False) & (previous_day['remove_term'] == False)]
    return data_to_get_activity

def build_activity_report(data_to_get_all_activity):
    """
    Function that builds an activity report
    :param data_to_get_all_activity:
    :return: A dataframe with activity being considered for course copy
    """
    data_to_get_all_activity['page_activity'] = data_to_get_all_activity.course_id.apply(get_page_activity)
    data_to_get_all_activity['assignment_activity'] = data_to_get_all_activity.course_id.apply(get_assignment_activity)
    data_to_get_all_activity['quiz_activity'] = data_to_get_all_activity.course_id.apply(get_quiz_activity)
    data_to_get_all_activity['module_activity'] = data_to_get_all_activity.course_id.apply(get_module_activity)
    data_to_get_all_activity['discussion_activity'] = data_to_get_all_activity.course_id.apply(get_discussion_activity)
    return data_to_get_all_activity

def filter_activity(report_with_activity):
    """
    Function that filters any course with activity
    :param report_with_activity:
    :return: A dataframe with filtered courses
    """
    filtered_activity_dataframe = report_with_activity.loc[(report_with_activity['page_activity'] <= 0) & (report_with_activity['assignment_activity'] <= 0) & (report_with_activity['quiz_activity'] <= 0) & (report_with_activity['module_activity'] <= 0) & (report_with_activity['discussion_activity'] <= 0)]
    return filtered_activity_dataframe

def course_copy(course_id):
    """
    Function that performs a course copy
    :param course_id:
    :return: A status code or message with status code for failures
    """
    course_copy_url = '{baseurl}/courses/sis_course_id:{courseid}/content_migrations?migration_type=course_copy_importer&settings[source_course_id]={sis_course}'.format(baseurl=base_url, sis_course=course_canvas_id_to_copy, courseid=course_id)
    r = requests.post(course_copy_url, headers=header)
    if r.ok:
        return r.status_code
    else:
        return 'failed with status code: {}'.format(r.status_code)

if __name__ == '__main__':
    main()
